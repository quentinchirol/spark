"""Traitement planifié : Spark recalcule les tables agrégées des dashboards (lake.dashboard.*).

Lancé chaque heure par le CronJob k8s/pipeline/cronjob.yaml, avec l'identité "pipeline" ;
à la main :

    python pipeline.py                                    # depuis lake.bench_1000000
    python pipeline.py --source lake.bench_10000000 --cible lake.dashboard
"""
import argparse
import os
import time

from lakehouse import agregats
from lakehouse.connexion import nouvelle_session, token_par_defaut
from lakehouse.donnees import lire_iceberg


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", default=os.environ.get("PIPELINE_SOURCE", "lake.bench_1000000"),
                        help="schéma Iceberg des tables détaillées (ventes, clients, produits)")
    parser.add_argument("--cible", default=os.environ.get("PIPELINE_CIBLE", agregats.NAMESPACE_DASHBOARD),
                        help="schéma Iceberg des tables agrégées")
    args = parser.parse_args()

    spark = nouvelle_session(token_par_defaut())
    try:
        debut = time.perf_counter()
        lignes = agregats.ecrire(spark, lire_iceberg(spark, args.source), args.cible)
        for nom, n in lignes.items():
            print(f"{args.cible}.{nom} : {n} lignes", flush=True)
        print(f"Terminé en {time.perf_counter() - debut:.1f} s (source {args.source})", flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
