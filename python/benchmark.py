"""Mesure le temps de chaque requête de requetes.py sur le cluster et exporte tout en CSV.

Trois sources sont comparées :
- cache   : Spark, tables générées puis mises en cache dans la mémoire des executors ;
- iceberg : Spark, tables Iceberg sur le stockage S3 (écrites une fois, réutilisées ensuite) ;
- trino   : Trino, mêmes tables Iceberg, requêtes SQL de requetes_sql.py.

    python benchmark.py                                  # 100k, 1M, 10M ventes, 3 sources
    python benchmark.py --tailles 100000 --repetitions 5 --sources iceberg trino
    python benchmark.py --recreer-iceberg                # réécrit les tables Iceberg

Une requête qui échoue (ex. mémoire ou durée maximale de Trino dépassée) est notée dans la
colonne "erreur" et n'arrête pas le benchmark.

Fichiers dans resultats/ (séparateur ";", décimales "," : s'ouvrent tels quels dans Excel) :
- benchmark_<date>.csv   : une ligne par exécution (préparation des données comprise) ;
- resume_<date>.csv      : à froid / min / médiane / moyenne / max par source, requête et taille ;
- comparaison_<date>.csv : les sources côte à côte : gain d'Iceberg sur le cache (Spark) et
                           gain de Trino sur Spark (mêmes tables Iceberg), en %.
"""
import argparse
import csv
import statistics
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from lakehouse.connexion import connexion_trino, lire_tokens, nouvelle_session, token_par_defaut
from lakehouse.donnees import ecrire_iceberg, generer, lire_iceberg, namespace_iceberg, tables_iceberg_existent
from lakehouse.requetes import REQUETES
from lakehouse.requetes_sql import REQUETES_SQL

DOSSIER_RESULTATS = Path(__file__).resolve().parent / "resultats"
SOURCES = ["cache", "iceberg", "trino"]
COLONNES = ["date", "session_id", "source", "requete", "nb_ventes", "execution", "a_froid",
            "duree_s", "nb_lignes_resultat", "nb_executors_max", "erreur"]


class CompteurExecutors:
    """Relève en arrière-plan le nombre maximal de pods executors actifs pendant une mesure."""

    def __init__(self, intervalle_s: float = 1.0):
        self.intervalle_s = intervalle_s
        self.maximum: int | None = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._boucle, daemon=True)

    @staticmethod
    def compter() -> int | None:
        try:
            sortie = subprocess.run(
                ["kubectl", "-n", "spark", "get", "pods", "-l", "spark-role=executor",
                 "--field-selector=status.phase=Running", "-o", "name"],
                capture_output=True, text=True, timeout=10, check=True,
            ).stdout
            return len(sortie.split())
        except (OSError, subprocess.SubprocessError):
            return None  # kubectl absent ou cluster inaccessible : colonne laissée vide

    def _boucle(self):
        while not self._stop.is_set():
            n = self.compter()
            if n is not None:
                self.maximum = max(self.maximum or 0, n)
            self._stop.wait(self.intervalle_s)

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join()


def nombre_fr(valeur) -> str:
    if valeur is None:
        return ""
    return f"{valeur:.3f}".replace(".", ",") if isinstance(valeur, float) else str(valeur)


def mesurer(action) -> tuple[float, int | None]:
    with CompteurExecutors() as compteur:
        debut = time.perf_counter()
        action()
        duree = time.perf_counter() - debut
    return duree, compteur.maximum


def executer(df):
    # force le calcul complet sur le cluster sans rapatrier le résultat sur le poste
    df.write.format("noop").mode("overwrite").save()


def executer_trino(curseur, sql: str):
    # EXPLAIN ANALYZE exécute toute la requête (toutes les colonnes) sans renvoyer ses lignes :
    # l'équivalent du format "noop" de Spark
    curseur.execute(f"EXPLAIN ANALYZE {sql}")
    curseur.fetchall()


def compter_trino(curseur, sql: str) -> int:
    curseur.execute(f"SELECT count(*) FROM ({sql})")
    return curseur.fetchall()[0][0]


def ecrire_csv(chemin: Path, entete: list[str], lignes: list[list]):
    with open(chemin, "w", newline="", encoding="utf-8-sig") as f:
        sortie = csv.writer(f, delimiter=";")
        sortie.writerow(entete)
        sortie.writerows([[nombre_fr(v) for v in ligne] for ligne in lignes])


def gain_pct(reference: float, mesure: float) -> float:
    """Gain de `mesure` par rapport à `reference` (positif = plus rapide)."""
    return (reference - mesure) / reference * 100


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--tailles", type=int, nargs="+", default=[100_000, 1_000_000, 10_000_000],
                        help="nombres de ventes à générer")
    parser.add_argument("--repetitions", type=int, default=3, help="exécutions à chaud par requête")
    parser.add_argument("--requetes", nargs="+", choices=list(REQUETES), default=list(REQUETES))
    parser.add_argument("--sources", nargs="+", choices=SOURCES, default=SOURCES)
    parser.add_argument("--recreer-iceberg", action="store_true",
                        help="réécrire les tables Iceberg même si elles existent déjà")
    args = parser.parse_args()

    DOSSIER_RESULTATS.mkdir(exist_ok=True)
    horodatage = datetime.now().strftime("%Y%m%d_%H%M%S")
    fichier_detail = DOSSIER_RESULTATS / f"benchmark_{horodatage}.csv"

    spark = nouvelle_session(token_par_defaut())
    session_id = getattr(spark, "session_id", "")
    curseur_trino = None
    if "trino" in args.sources:
        utilisateur, token = next(iter(lire_tokens().items()))
        curseur_trino = connexion_trino(utilisateur, token).cursor()
    print(f"Session {session_id} - résultats : {fichier_detail}", flush=True)
    durees: dict[tuple[str, str, int], list[float]] = {}
    a_froid: dict[tuple[str, str, int], float] = {}

    with open(fichier_detail, "w", newline="", encoding="utf-8-sig") as f:
        sortie = csv.writer(f, delimiter=";")
        sortie.writerow(COLONNES)

        def ecrire(source, requete, taille, execution, froid, duree, nb_lignes, nb_executors, erreur=""):
            sortie.writerow([datetime.now().isoformat(timespec="seconds"), session_id, source, requete,
                             taille, execution, "oui" if froid else "non", nombre_fr(duree),
                             nombre_fr(nb_lignes), nombre_fr(nb_executors), erreur])
            f.flush()  # les mesures déjà faites sont conservées même si le script est interrompu
            if erreur:
                print(f"  {source:<8} {requete:<20} ÉCHEC : {erreur}", flush=True)
            else:
                print(f"  {source:<8} {requete:<20} n°{execution} {'froid' if froid else 'chaud'}"
                      f"  {duree:8.3f} s  executors={nb_executors}", flush=True)

        for taille in args.tailles:
            print(f"\n== {taille:,} ventes ==".replace(",", " "), flush=True)
            generees = generer(spark, taille)
            namespace = namespace_iceberg(taille)
            if {"iceberg", "trino"} & set(args.sources) and (
                    args.recreer_iceberg or not tables_iceberg_existent(spark, namespace)):
                duree, nb_exec = mesurer(lambda: ecrire_iceberg(spark, generees, namespace))
                ecrire("iceberg", "preparation", taille, 1, True, duree, taille, nb_exec)

            for source in args.sources:
                if source == "cache":
                    def preparer():
                        for df in generees.values():
                            df.cache().count()
                    duree, nb_exec = mesurer(preparer)
                    ecrire(source, "preparation", taille, 1, True, duree, taille, nb_exec)
                    tables = generees
                elif source == "iceberg":
                    tables = lire_iceberg(spark, namespace)

                try:
                    for nom in args.requetes:
                        if source == "trino":
                            sql = REQUETES_SQL[nom](namespace)
                            compter = lambda: compter_trino(curseur_trino, sql)  # noqa: E731
                            lancer = lambda: executer_trino(curseur_trino, sql)  # noqa: E731
                        else:
                            compter = lambda: REQUETES[nom](tables).count()  # noqa: E731
                            lancer = lambda: executer(REQUETES[nom](tables))  # noqa: E731
                        try:
                            nb_lignes = compter()  # non chronométré
                            for execution in range(1, args.repetitions + 2):
                                froid = execution == 1
                                duree, nb_exec = mesurer(lancer)
                                if source == "trino":
                                    nb_exec = None  # executors Spark : sans objet pour Trino
                                ecrire(source, nom, taille, execution, froid, duree, nb_lignes, nb_exec)
                                cle = (source, nom, taille)
                                if froid:
                                    a_froid[cle] = duree
                                else:
                                    durees.setdefault(cle, []).append(duree)
                        except Exception as e:  # noqa: BLE001 - une requête en échec n'arrête pas le reste
                            message = str(e).splitlines()[0][:300] if str(e) else type(e).__name__
                            ecrire(source, nom, taille, "", True, None, None, None, message)
                finally:
                    if source == "cache":
                        for df in generees.values():
                            df.unpersist()

    mediane = {cle: statistics.median(v) for cle, v in durees.items()}
    ecrire_csv(
        DOSSIER_RESULTATS / f"resume_{horodatage}.csv",
        ["source", "requete", "nb_ventes", "a_froid_s", "min_s", "mediane_s", "moyenne_s", "max_s"],
        [[s, n, t, a_froid[(s, n, t)], min(v), mediane[(s, n, t)], statistics.mean(v), max(v)]
         for (s, n, t), v in durees.items()],
    )

    def m(source, nom, taille):
        return mediane.get((source, nom, taille))

    def gain(reference, mesure):
        return round(gain_pct(reference, mesure), 1) if reference and mesure else None

    print("\nMédiane à chaud (s) ; vide = échec :")
    for source in args.sources:
        print(f"\n[{source}]\n{'requete':<20}" + "".join(f"{t:>12}" for t in args.tailles))
        for nom in args.requetes:
            print(f"{nom:<20}" + "".join(
                f"{m(source, nom, t):>12.3f}" if m(source, nom, t) else f"{'—':>12}" for t in args.tailles))

    ecrire_csv(
        DOSSIER_RESULTATS / f"comparaison_{horodatage}.csv",
        ["requete", "nb_ventes", "cache_mediane_s", "iceberg_mediane_s", "trino_mediane_s",
         "gain_iceberg_sur_cache_pct", "gain_trino_sur_spark_iceberg_pct"],
        [[nom, t, m("cache", nom, t), m("iceberg", nom, t), m("trino", nom, t),
          gain(m("cache", nom, t), m("iceberg", nom, t)), gain(m("iceberg", nom, t), m("trino", nom, t))]
         for t in args.tailles for nom in args.requetes],
    )
    for titre, ref, autre in [("Gain d'Iceberg sur le cache (Spark)", "cache", "iceberg"),
                              ("Gain de Trino sur Spark, mêmes tables Iceberg", "iceberg", "trino")]:
        if {ref, autre} <= set(args.sources):
            print(f"\n{titre} (positif = plus rapide) :")
            print(f"{'requete':<20}" + "".join(f"{t:>12}" for t in args.tailles))
            for nom in args.requetes:
                print(f"{nom:<20}" + "".join(
                    f"{gain(m(ref, nom, t), m(autre, nom, t)):>+11.0f}%" if gain(m(ref, nom, t), m(autre, nom, t)) is not None
                    else f"{'—':>12}" for t in args.tailles))

    print(f"\nFichiers dans {DOSSIER_RESULTATS}")
    spark.stop()


if __name__ == "__main__":
    main()
