#!/usr/bin/env python3
"""Accès temps réel exceptionnel (US-16) : accorder, lister, retirer.

Un utilisateur temps réel lit les vues vue.* à la seconde près sur une RÉPLIQUE du PostgreSQL principal, jamais
sur le primaire (docs/temps-reel.md). L'accès est personnel et limité dans le temps (7 jours au plus).

    python scripts/temps-reel.py accorder alice --duree 8h      # 30m, 8h, 2j…
    python scripts/temps-reel.py liste
    python scripts/temps-reel.py retirer alice

Le mot de passe est généré ici et rangé dans le Secret temps-reel-<nom> : il n'est jamais affiché ni passé en
argument d'une commande, et PostgreSQL ne reçoit que son empreinte SCRAM (rien en clair dans ses journaux ni dans
pg_stat_statements). Nécessite kubectl, avec les droits d'administration de l'espace de noms (exec dans les pods
PostgreSQL, Secrets) ; variable NAMESPACE (spark par défaut).
"""
import argparse
import base64
import datetime
import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys

NS = os.environ.get("NAMESPACE", "spark")
CLUSTER = "lake-principal"
GROUPE = "temps_reel"
DUREE_MAX = datetime.timedelta(days=7)
CONNEXIONS_MAX = 2
# limites de chaque utilisateur temps réel (sur les répliques) : requêtes courtes, aucune transaction qui traîne
# (une transaction longue sur une réplique peut retenir le nettoyage du primaire)
LIMITES = {
    "statement_timeout": "30s",
    "work_mem": "16MB",
    "idle_in_transaction_session_timeout": "1min",
    "idle_session_timeout": "15min",
    "default_transaction_read_only": "on",
}
UNITES = {"m": "minutes", "h": "hours", "j": "days"}


def kubectl(*args, entree=None, verifier=True):
    resultat = subprocess.run(["kubectl", "-n", NS, *args], input=entree, text=True, capture_output=True,
                              check=False)
    if verifier and resultat.returncode != 0:
        sys.exit(f"échec de kubectl {args[0]} : {resultat.stderr.strip()}")
    return resultat.stdout


def pods(role=None):
    selecteur = f"cluster-name={CLUSTER}" + (f",spilo-role={role}" if role else "")
    return kubectl("get", "pods", "-l", selecteur, "-o", "name").split()


def primaire():
    noms = pods("master")
    if len(noms) != 1:
        sys.exit(f"primaire du PostgreSQL principal introuvable ({' '.join(noms) or 'aucun pod'})")
    return noms[0]


def sql(pod, requete):
    """Requête en superutilisateur dans le pod, par l'entrée standard (jamais en argument de commande)."""
    return kubectl("exec", "-i", pod, "-c", "postgres", "--", "psql", "-U", "postgres", "-d", "lake", "-X", "-q",
                   "-tA", "-v", "ON_ERROR_STOP=1", entree=requete).strip()


def utilisateur(nom):
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,40}", nom):
        sys.exit(f"nom invalide : {nom!r} (minuscules, chiffres, _ ; commence par une lettre)")
    return f"{GROUPE}_{nom}"


def secret(nom):
    return f"temps-reel-{nom.replace('_', '-')}"


def duree(texte):
    correspondance = re.fullmatch(r"(\d+)([mhj])", texte)
    if not correspondance:
        raise argparse.ArgumentTypeError(f"durée invalide : {texte!r} (ex. 30m, 8h, 2j)")
    valeur = datetime.timedelta(**{UNITES[correspondance[2]]: int(correspondance[1])})
    if not datetime.timedelta(minutes=1) <= valeur <= DUREE_MAX:
        raise argparse.ArgumentTypeError("durée entre 1 minute et 7 jours (accès exceptionnel)")
    return valeur


def empreinte_scram(mot_de_passe, iterations=4096):
    """Empreinte SCRAM-SHA-256 au format de PostgreSQL : le serveur ne voit jamais le mot de passe."""
    sel = secrets.token_bytes(16)
    sale = hashlib.pbkdf2_hmac("sha256", mot_de_passe.encode(), sel, iterations)
    cle_client = hmac.new(sale, b"Client Key", "sha256").digest()
    cle_serveur = hmac.new(sale, b"Server Key", "sha256").digest()

    def b64(octets):
        return base64.b64encode(octets).decode()
    return f"SCRAM-SHA-256${iterations}:{b64(sel)}${b64(hashlib.sha256(cle_client).digest())}:{b64(cle_serveur)}"


def requete_accorder(role, empreinte, fin):
    limites = "\n".join(f"ALTER ROLE {role} SET {cle} = '{valeur}';" for cle, valeur in LIMITES.items())
    return f"""
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{GROUPE}') THEN
        RAISE EXCEPTION 'rôle {GROUPE} absent : déployer k8s/postgres/sql/05-temps-reel.sql (Job lake-schema)';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '{role}') THEN
        CREATE ROLE {role};
    END IF;
END $$;
ALTER ROLE {role} LOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT {CONNEXIONS_MAX} PASSWORD '{empreinte}' VALID UNTIL '{fin.isoformat()}';
GRANT {GROUPE} TO {role};
{limites}
"""


def accorder(nom, valeur_duree):
    role = utilisateur(nom)
    fin = (datetime.datetime.now(datetime.timezone.utc) + valeur_duree).replace(microsecond=0)
    mot_de_passe = secrets.token_urlsafe(24)
    sql(primaire(), requete_accorder(role, empreinte_scram(mot_de_passe), fin))
    # create (et non apply) : apply garderait une copie du manifeste, mot de passe compris, en annotation
    kubectl("delete", "secret", secret(nom), "--ignore-not-found")
    kubectl("create", "-f", "-", entree=json.dumps({
        "apiVersion": "v1", "kind": "Secret", "type": "Opaque",
        "metadata": {"name": secret(nom), "labels": {"app": "temps-reel"}},
        "stringData": {"utilisateur": role, "mot_de_passe": mot_de_passe, "expire": fin.isoformat(),
                       "hote": f"{CLUSTER}-repl", "base": "lake"},
    }))
    print(f"accès temps réel accordé : {role}, jusqu'au {fin:%Y-%m-%d %H:%M} UTC")
    print(f"mot de passe : Secret {secret(nom)} → "
          f"kubectl -n {NS} get secret {secret(nom)} -o jsonpath='{{.data.mot_de_passe}}' | base64 -d")
    print(f"connexion (répliques uniquement) : kubectl -n {NS} port-forward svc/{CLUSTER}-repl 5433:5432, "
          f"puis hôte localhost, port 5433, base lake, utilisateur {role}")


def liste():
    lignes = sql(primaire(), f"""
SELECT r.rolname, coalesce(to_char(r.rolvaliduntil AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') || ' UTC', 'sans fin'),
       CASE WHEN NOT r.rolcanlogin THEN 'désactivé' WHEN r.rolvaliduntil < now() THEN 'expiré' ELSE 'actif' END
FROM pg_roles r
JOIN pg_auth_members m ON m.member = r.oid
JOIN pg_roles g ON g.oid = m.roleid AND g.rolname = '{GROUPE}'
ORDER BY 1""")
    if not lignes:
        print("aucun accès temps réel")
        return
    print(f"{'utilisateur':<40} {'jusqu’au':<22} état")
    for ligne in lignes.splitlines():
        role, fin, etat = ligne.split("|")
        print(f"{role:<40} {fin:<22} {etat}")


def retirer(nom):
    role = utilisateur(nom)
    pod_primaire = primaire()
    existe = sql(pod_primaire, f"SELECT count(*) FROM pg_roles WHERE rolname = '{role}'") == "1"
    if existe:
        # plus de nouvelle connexion (répliquée aussitôt), puis sessions coupées sur chaque instance, puis suppression
        sql(pod_primaire, f"ALTER ROLE {role} NOLOGIN;")
        for pod in pods():
            sql(pod, f"SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE usename = '{role}';")
        sql(pod_primaire, f"REVOKE {GROUPE} FROM {role}; DROP ROLE {role};")
    kubectl("delete", "secret", secret(nom), "--ignore-not-found")
    print(f"accès temps réel retiré : {role}" if existe else f"aucun accès temps réel pour {role} (Secret supprimé)")


def main():
    analyse = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    commandes = analyse.add_subparsers(dest="commande", required=True)
    commande = commandes.add_parser("accorder", help="crée ou renouvelle l'accès (nouveau mot de passe)")
    commande.add_argument("nom")
    commande.add_argument("--duree", type=duree, default=duree("8h"), help="30m, 8h, 2j… (8h par défaut, 7j au plus)")
    commandes.add_parser("liste", help="accès temps réel existants")
    commande = commandes.add_parser("retirer", help="coupe les sessions et supprime l'accès")
    commande.add_argument("nom")
    arguments = analyse.parse_args()
    if arguments.commande == "accorder":
        accorder(arguments.nom, arguments.duree)
    elif arguments.commande == "liste":
        liste()
    else:
        retirer(arguments.nom)


if __name__ == "__main__":
    main()
