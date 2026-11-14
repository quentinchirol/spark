# Accès temps réel exceptionnel : à la seconde, sur une réplique

Le pool analytique ([analytique.md](analytique.md)) montre les données de l'application avec moins de 2 minutes de
retard, ce qui suffit presque toujours. Pour **suivre un événement critique à la seconde près** (un incident, un
lancement), un responsable métier reçoit un accès **personnel et temporaire** : il lit les vues `vue.*` sur une
**réplique** du PostgreSQL principal. Le primaire, qui reçoit les écritures de l'application, n'est jamais sollicité.

```
responsable métier ──PostgreSQL──► lake-principal-repl : répliques (lecture seule)
(accès temps réel)                     ▲ réplication synchrone : écriture de l'application visible en moins d'1 s
                                       │
                     ✗ refusé ──► lake-principal : primaire (écritures de l'application)
```

## Accorder, lister, retirer (administrateur de la plateforme)

```bash
python scripts/temps-reel.py accorder alice --duree 8h     # 30m, 8h, 2j… : 8 h par défaut, 7 jours au plus
python scripts/temps-reel.py liste                         # utilisateurs, fin de l'accès, état (actif, expiré)
python scripts/temps-reel.py retirer alice                 # sessions coupées tout de suite, accès supprimé
```

- utilisateur PostgreSQL `temps_reel_alice`, membre du rôle `temps_reel` ; relancer `accorder` renouvelle l'accès
  avec un **nouveau** mot de passe ;
- le mot de passe est généré par le script et rangé dans le Secret `temps-reel-alice` : jamais affiché, jamais en
  argument d'une commande ; PostgreSQL ne reçoit que son empreinte SCRAM (rien en clair dans ses journaux) ;
- à l'expiration, toute nouvelle connexion est refusée ; `retirer` nettoie (rôle et Secret).

Nécessite `kubectl` avec les droits d'administration de l'espace de noms `spark` (variable `NAMESPACE` sinon).

## Se connecter (personne autorisée)

```bash
kubectl -n spark port-forward svc/lake-principal-repl 5433:5432        # répliques uniquement
kubectl -n spark get secret temps-reel-alice -o jsonpath='{.data.mot_de_passe}' | base64 -d   # ou transmis par l'administrateur
psql "host=localhost port=5433 dbname=lake user=temps_reel_alice sslmode=require"
```

DBeaver ou tout client PostgreSQL : hôte `localhost`, port `5433`, base `lake`, SSL requis. Exemples :

```sql
SELECT * FROM vue.paiements WHERE id = '…';                              -- dernier état, à la seconde
SELECT * FROM vue.commandes_historique WHERE id = '…' ORDER BY _version;  -- tous les changements d'une ligne
SELECT pg_is_in_recovery();                                              -- true : je suis sur une réplique
```

| Lisible | Non lisible |
|---------|-------------|
| `vue.<table>` (dernier état, chaud + Iceberg), `vue.<table>_historique` ; `app.<table>` (état fusionné) | tables chaudes `chaud.*`, fonctions d'écriture, tout le reste ; aucune écriture (réplique) |

## Garanties

| Garantie | Comment |
|----------|---------|
| **jamais le primaire** | 1. connexion par le Service des répliques ; 2. une connexion d'un membre de `temps_reel` au primaire est **refusée** avant toute requête (déclencheur de connexion PostgreSQL 18, bases `lake`, `postgres`, `template1` et toute base créée ensuite) ; 3. si une réplique est promue primaire pendant une session (bascule Patroni), `pg_cron` coupe la session en moins de 30 s |
| à la seconde | réplication synchrone : une écriture confirmée à l'application est déjà sur une réplique ; lisible sur la réplique en quelques millisecondes |
| refusé sans cet accès | aucun droit sur `vue.*` ni `app.*` pour les autres utilisateurs ; accès expiré ou retiré : connexion refusée |
| exceptionnel | accès nominatif, 7 jours au plus, listé (`liste`) |
| les répliques restent saines | 2 connexions par personne, requêtes de 30 s au plus, `work_mem` 16 Mo, transaction inactive coupée après 1 min et session inactive après 15 min : une transaction longue sur une réplique retiendrait le nettoyage du primaire |

**Aucune réplique disponible** (panne, redémarrage) : le Service des répliques n'a plus de point d'accès et la
connexion échoue ; on ne se rabat **jamais** sur le primaire. Patroni recrée les répliques ; réessayer ensuite.

## Fichiers

| Fichier | Rôle |
|---------|------|
| `k8s/postgres/sql/05-temps-reel.sql` | rôle `temps_reel` et ses droits, garde dans chaque base, coupure planifiée (Job `lake-schema`) |
| `k8s/postgres/sql/garde-temps-reel.psql` | déclencheur de connexion : refus d'un membre sur le primaire |
| `scripts/temps-reel.py` | accorder, lister, retirer |
| `tests/sql/temps-reel.sql` | garde sur un primaire (CI « SQL ») |
| `tests/integration/tester_temps_reel.py` | réplique, refus, expiration, bascule, retrait (CI « PostgreSQL principal », kind) |

## Exploitation

```bash
# coupure planifiée sur le primaire et ses dernières exécutions
kubectl -n spark exec -it $(kubectl -n spark get pods -l cluster-name=lake-principal,spilo-role=master -o name) \
  -c postgres -- psql -U postgres -d postgres -c \
  "SELECT status, return_message, end_time FROM cron.job_run_details d JOIN cron.job j USING (jobid)
   WHERE j.jobname = 'temps-reel-hors-primaire' ORDER BY end_time DESC LIMIT 5"
```

Une erreur dans la fonction de garde empêcherait les connexions à la base : un superutilisateur se connecte toujours
avec `PGOPTIONS='-c event_triggers=off'` (paramètre réservé aux superutilisateurs) pour la corriger. La fonction ne
s'applique jamais aux superutilisateurs (Patroni, opérateur, Job `lake-schema`) et ne fait rien sur les répliques.

## Mesuré en CI (kind)

`tests/integration/tester_temps_reel.py` (job « PostgreSQL principal ») :

| Contrôle | Résultat |
|----------|----------|
| session d'un utilisateur temps réel | sur une réplique (`pg_is_in_recovery()`), `vue.commandes` lue (chaud + Iceberg) |
| écriture de l'application → lisible sur la réplique | **12 ms** (médiane), 13 ms au plus, sur 10 écritures |
| même utilisateur sur le primaire (`lake`, `postgres`, `template1`) | refusé à la connexion |
| utilisateur sans accès | `permission denied` sur `vue.*`, `app.*`, `chaud.*` |
| accès expiré | connexion refusée |
| session de membre restée sur le primaire (bascule simulée) | coupée après 24 s |
| `retirer` | session ouverte coupée, connexion refusée, Secret supprimé |
