"""Test d'intégration de l'accès temps réel exceptionnel (US-16), sur kind, après tester_postgres.py.

  1. accès accordé (scripts/temps-reel.py) : la requête lit une RÉPLIQUE du PostgreSQL principal, à la seconde près
     (écriture de l'application visible en moins d'une seconde), en lecture seule, avec ses limites ;
  2. le même utilisateur est refusé sur le primaire, quelle que soit la base ;
  3. un utilisateur sans cet accès est refusé (aucun droit sur les vues ni les tables) ;
  4. un accès expiré est refusé ;
  5. une session de membre présente sur le primaire (cas d'une réplique promue par une bascule) est coupée en
     moins de 30 s ;
  6. accès retiré : sessions coupées, connexion refusée, Secret supprimé.

    python tests/integration/tester_temps_reel.py
Dépendances : pip install "psycopg[binary]"
"""
import base64
import importlib.util
import json
import secrets
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

import psycopg

from commun import kubectl, tunnel, verifier

RACINE = Path(__file__).resolve().parents[2]
PORT_REPLIQUE, PORT_PRIMAIRE = 15436, 15437
NOM = "essai"
UTILISATEUR = f"temps_reel_{NOM}"

# scripts/temps-reel.py (nom avec un tiret) : chargé comme module pour réutiliser son empreinte SCRAM
_spec = importlib.util.spec_from_file_location("temps_reel", RACINE / "scripts" / "temps-reel.py")
temps_reel = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(temps_reel)


def script(*args):
    resultat = subprocess.run([sys.executable, str(RACINE / "scripts" / "temps-reel.py"), *args],
                              text=True, capture_output=True, check=False)
    print("     " + "\n     ".join(resultat.stdout.strip().splitlines()), flush=True)
    verifier(resultat.returncode == 0, f"scripts/temps-reel.py {' '.join(args)} {resultat.stderr.strip()}")
    return resultat.stdout


def secret(nom, cle):
    """Valeur d'un Secret, jamais affichée."""
    return base64.b64decode(kubectl("get", "secret", nom, "-o", f"jsonpath={{.data.{cle}}}")).decode()


def sql_superutilisateur(requete):
    """Requête en superutilisateur dans le primaire, par l'entrée standard."""
    return temps_reel.sql(temps_reel.primaire(), requete)


def connexion(port, utilisateur, mot_de_passe, base="lake"):
    return psycopg.connect(host="localhost", port=port, dbname=base, user=utilisateur, password=mot_de_passe,
                           sslmode="require", autocommit=True, connect_timeout=10)


def refus(port, utilisateur, mot_de_passe, base="lake"):
    """Motif du refus de connexion, ou None si la connexion est acceptée."""
    try:
        connexion(port, utilisateur, mot_de_passe, base).close()
        return None
    except psycopg.OperationalError as erreur:
        return " ".join(str(erreur).split())


def erreur(conn, requete):
    try:
        conn.execute(requete).fetchall()
        return None
    except psycopg.Error as e:
        return str(e).splitlines()[0]


def main():
    tunnels = [tunnel("svc/lake-principal-repl", PORT_REPLIQUE, 5432),
               tunnel(temps_reel.primaire(), PORT_PRIMAIRE, 5432)]
    superutilisateur = connexion(PORT_PRIMAIRE, "postgres",
                                 secret("postgres.lake-principal.credentials.postgresql.acid.zalan.do", "password"))
    try:
        print("1. Accès accordé : lecture sur une réplique, à la seconde près")
        script("accorder", NOM, "--duree", "1h")
        verifier(UTILISATEUR in script("liste"), "accès listé")
        mot_de_passe = secret(f"temps-reel-{NOM}", "mot_de_passe")
        conn = connexion(PORT_REPLIQUE, UTILISATEUR, mot_de_passe)
        verifier(conn.execute("SELECT pg_is_in_recovery()").fetchone()[0], "la session est sur une réplique")
        lignes = conn.execute("SELECT count(*) FROM vue.commandes").fetchone()[0]
        verifier(lignes > 0, f"vue.commandes lue sur la réplique ({lignes} lignes, chaud + Iceberg)")
        retards = []
        for _ in range(10):
            id_ = str(uuid.uuid4())
            message = [{"id": id_, "op": "INSERT", "table": "commandes", "date_creation": "2026-11-14",
                        "version": 1, "donnees": {"client": "temps-reel", "montant": 1, "statut": "ouverte"}}]
            superutilisateur.execute("SELECT lake.ecrire_lot('commandes', %s::jsonb)", (json.dumps(message),))
            ecrit = time.monotonic()
            while not conn.execute("SELECT count(*) FROM vue.commandes WHERE id = %s", (id_,)).fetchone()[0]:
                if time.monotonic() - ecrit > 5:
                    break
                time.sleep(0.01)
            retards.append(time.monotonic() - ecrit)
        verifier(max(retards) < 1, f"écriture de l'application lue sur la réplique après {statistics.median(retards) * 1000:.0f} ms "
                                   f"(médiane), {max(retards) * 1000:.0f} ms au plus, sur 10 écritures")
        verifier(conn.execute("SHOW statement_timeout").fetchone()[0] == "30s", "requêtes limitées à 30 s")
        verifier(erreur(conn, "CREATE TABLE public.essai (n int)") is not None, "écriture refusée (lecture seule)")
        verifier(erreur(conn, "SELECT count(*) FROM chaud.commandes") is not None,
                 "tables chaudes non lisibles directement : seules les vues")

        print("2. Le même utilisateur est refusé sur le primaire, dans toutes les bases")
        for base in ("lake", "postgres", "template1"):
            motif = refus(PORT_PRIMAIRE, UTILISATEUR, mot_de_passe, base)
            verifier(motif is not None and "refusée sur le primaire" in motif, f"base {base} : {motif}")

        print("3. Utilisateur sans accès temps réel : refusé")
        autre = secrets.token_urlsafe(18)
        sql_superutilisateur(f"DROP ROLE IF EXISTS essai_sans_acces; "
                             f"CREATE ROLE essai_sans_acces LOGIN PASSWORD '{temps_reel.empreinte_scram(autre)}';")
        with connexion(PORT_REPLIQUE, "essai_sans_acces", autre) as sans_acces:
            for objet in ("vue.commandes", "app.commandes", "chaud.commandes"):
                motif = erreur(sans_acces, f"SELECT count(*) FROM {objet}")
                verifier(motif is not None and "permission denied" in motif, f"{objet} : {motif}")

        print("4. Accès expiré : refusé")
        sql_superutilisateur(f"ALTER ROLE {UTILISATEUR} VALID UNTIL '2000-01-01 00:00:00+00';")
        time.sleep(1)
        motif = refus(PORT_REPLIQUE, UTILISATEUR, mot_de_passe)
        verifier(motif is not None, f"connexion refusée après l'expiration ({motif})")
        script("accorder", NOM, "--duree", "1h")      # renouvelé : nouveau mot de passe
        mot_de_passe = secret(f"temps-reel-{NOM}", "mot_de_passe")

        print("5. Session de membre sur le primaire (après une bascule) : coupée")
        sql_superutilisateur("DROP ROLE IF EXISTS essai_bascule; "
                             f"CREATE ROLE essai_bascule LOGIN PASSWORD '{temps_reel.empreinte_scram(autre)}';")
        bascule = connexion(PORT_PRIMAIRE, "essai_bascule", autre)    # pas encore membre : acceptée
        sql_superutilisateur("GRANT temps_reel TO essai_bascule;")
        debut, coupee = time.monotonic(), None
        while time.monotonic() - debut < 60:
            if erreur(bascule, "SELECT 1") is not None:
                coupee = time.monotonic() - debut
                break
            time.sleep(1)
        verifier(coupee is not None and coupee <= 40, f"session coupée sur le primaire après {coupee or 0:.0f} s")
        sql_superutilisateur("DROP ROLE essai_bascule; DROP ROLE essai_sans_acces;")

        print("6. Accès retiré : sessions coupées, connexion refusée")
        ouverte = connexion(PORT_REPLIQUE, UTILISATEUR, mot_de_passe)
        verifier(erreur(ouverte, "SELECT count(*) FROM vue.commandes") is None, "session ouverte sur la réplique")
        script("retirer", NOM)
        verifier(erreur(ouverte, "SELECT 1") is not None, "session en cours coupée")
        verifier(refus(PORT_REPLIQUE, UTILISATEUR, mot_de_passe) is not None, "nouvelle connexion refusée")
        verifier(not kubectl("get", "secret", "-l", "app=temps-reel", "-o", "name").strip(), "Secret supprimé")
    finally:
        superutilisateur.close()
        for t in tunnels:
            t.terminate()
    print("Accès temps réel : tous les contrôles passent")


if __name__ == "__main__":
    main()
