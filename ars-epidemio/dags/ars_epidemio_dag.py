from typing import Any, Dict
import os
import shutil
import sys
import json
import logging
from datetime import datetime, timedelta


from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable

logger = logging.getLogger(__name__)

SEMAINE_FIXE = "2024-S16"

# Fonctions
def verifier_connexions() -> None:
    try:
        conn_pg = BaseHook.get_connection("postgres_ars")

        depts = Variable.get("departements_occitanie", deserialize_json=True)
        syndromes = Variable.get("syndromes_surveilles", deserialize_json=True)

        logger.info(f"POSTGRES_CONN:{conn_pg.host}:{conn_pg.port}/{conn_pg.schema}")
        logger.info(f"NB_DEPARTEMENTS:{len(depts)}")
        logger.info(f"NB_SYNDROMES:{len(syndromes)}")
    except Exception:
        logger.exception("ERREUR_VERIFIER_CONNEXIONS")
        raise


def collecter_donnees_ias(**context: Dict[str, Any]) -> str:
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"

        archive_path = Variable.get("archive_base_path", default_var="/data/ars")
        output_dir = f"{archive_path}/raw"

        sys.path.insert(0, "/opt/airflow/scripts")

        from collecte_ias import (
            DATASETS_IAS,
            telecharger_csv_ias,
            filtrer_semaine,
            agreger_semaine,
            sauvegarder_donnees,
        )

        resultats = {}

        for syndrome, url in DATASETS_IAS.items():
            rows_all = telecharger_csv_ias(url)
            rows_sem = filtrer_semaine(rows_all, semaine)
            resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

        path = sauvegarder_donnees(resultats, semaine, output_dir)
        logger.info(f"COLLECTE_OK:{path}")
        return path
    except Exception:
        logger.exception("ERREUR_COLLECTE_IAS")
        raise


def archiver_local(**context: Dict[str, Any]) -> str:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]

        chemin_source = context["ti"].xcom_pull(task_ids="collecter_donnees_sursaud")

        archive_dir = f"/data/ars/raw/{annee}/{num_sem}"
        os.makedirs(archive_dir, exist_ok=True)

        chemin_dest = f"{archive_dir}/sursaud_{semaine}.json"

        shutil.copy2(chemin_source, chemin_dest)

        logger.info(f"ARCHIVE_OK:{chemin_dest}")
        return chemin_dest
    except Exception:
        logger.exception("ERREUR_ARCHIVAGE")
        raise


def verifier_archive(**context: Dict[str, Any]) -> bool:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]

        chemin = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

        if not os.path.exists(chemin):
            raise FileNotFoundError(f"Archive manquante : {chemin}")

        taille = os.path.getsize(chemin)

        if taille == 0:
            raise ValueError(f"Archive vide : {chemin}")

        logger.info(f"ARCHIVE_VALIDE:{chemin} ({taille} octets)")
        return True
    except Exception:
        logger.exception("ERREUR_VERIFICATION_ARCHIVE")
        raise


def calculer_indicateurs_epidemiques(**context: Dict[str, Any]) -> Any:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]


        chemin_json = f"/data/ars/raw/{annee}/{num_sem}/sursaud_{semaine}.json"

        sys.path.insert(0, "/opt/airflow/scripts")

        from calcul_indicateurs import calculer_indicateurs_depuis_json

        result = calculer_indicateurs_depuis_json(chemin_json)
        logger.info(f"INDICATEURS_OK:{semaine}")
        return result
    except Exception:
        logger.exception("ERREUR_CALCUL_INDICATEURS")
        raise


def inserer_donnees_postgres(**context: Dict[str, Any]) -> None:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]

        chemin_json = f"/data/ars/raw/{semaine.split('-')[0]}/{semaine.split('-')[1]}/sursaud_{semaine}.json"

        with open(chemin_json, "r", encoding="utf-8") as f:
            data = json.load(f)

        hook = PostgresHook(postgres_conn_id="postgres_ars")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                for syndrome, infos in data["syndromes"].items():
                    cur.execute("""
                        INSERT INTO donnees_hebdomadaires (
                            semaine, syndrome, valeur_ias,
                            seuil_min_saison, seuil_max_saison, nb_jours_donnees
                        )
                        VALUES (%(semaine)s, %(syndrome)s, %(valeur_ias)s,
                                %(seuil_min)s, %(seuil_max)s, %(nb_jours)s)
                        ON CONFLICT (semaine, syndrome)
                        DO UPDATE SET
                            valeur_ias = EXCLUDED.valeur_ias,
                            seuil_min_saison = EXCLUDED.seuil_min_saison,
                            seuil_max_saison = EXCLUDED.seuil_max_saison,
                            nb_jours_donnees = EXCLUDED.nb_jours_donnees,
                            updated_at = CURRENT_TIMESTAMP;
                    """, {
                        "semaine": semaine,
                        "syndrome": syndrome,
                        "valeur_ias": infos.get("valeur_ias"),
                        "seuil_min": infos.get("seuil_min"),
                        "seuil_max": infos.get("seuil_max"),
                        "nb_jours": infos.get("nb_jours"),
                    })
            conn.commit()

        logger.info(f"DONNEES_HBD_OK:{semaine}")
    except Exception:
        logger.exception("ERREUR_INSERT_POSTGRES")
        raise


def evaluer_situation_epidemique(**context: Dict[str, Any]) -> str:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]

        hook = PostgresHook(postgres_conn_id="postgres_ars")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT statut, COUNT(*) 
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    GROUP BY statut
                """, (semaine,))
                resultats = {row[0]: row[1] for row in cur.fetchall()}

        nb_urgence = resultats.get("URGENCE", 0)
        nb_alerte = resultats.get("ALERTE", 0)

        context["ti"].xcom_push(key="nb_urgence", value=nb_urgence)
        context["ti"].xcom_push(key="nb_alerte", value=nb_alerte)

        logger.info(f"SITUATION:{semaine} URGENCE={nb_urgence} ALERTE={nb_alerte}")

        if nb_urgence > 0:
            return "declencher_alerte_ars"
        elif nb_alerte > 0:
            return "envoyer_bulletin_surveillance"
        else:
            return "confirmer_situation_normale"
    except Exception:
        logger.exception("ERREUR_EVALUATION_SITUATION")
        raise


def declencher_alerte_ars(**context: Dict[str, Any]) -> None:
    try:
        nb = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_urgence")
        logger.critical(f"ALERTE_ARS:{nb}")
    except Exception:
        logger.exception("ERREUR_ALERTE_ARS")
        raise


def envoyer_bulletin_surveillance(**context: Dict[str, Any]) -> None:
    try:
        nb = context["ti"].xcom_pull(task_ids="evaluer_situation_epidemique", key="nb_alerte")
        logger.warning(f"BULLETIN_ALERTE:{nb}")
    except Exception:
        logger.exception("ERREUR_BULLETIN")
        raise


def confirmer_situation_normale(**context: Dict[str, Any]) -> None:
    try:
        logger.info("SITUATION_NORMALE")
    except Exception:
        logger.exception("ERREUR_SITUATION_NORMALE")
        raise


def generer_rapport_hebdomadaire(**context: Dict[str, Any]) -> None:
    '''
        semaine = SEMAINE_FIXE
        annee, num_sem = semaine.split("-")
    '''
    try:
        date_ref = context["data_interval_start"]
        semaine = f"{date_ref.year}-S{date_ref.isocalendar()[1]:02d}"
        annee = semaine.split("-")[0]
        num_sem = semaine.split("-")[1]

        hook = PostgresHook(postgres_conn_id="postgres_ars")

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT syndrome, valeur_ias, z_score, r0_estime, statut
                    FROM indicateurs_epidemiques
                    WHERE semaine = %s
                    ORDER BY statut DESC, valeur_ias DESC
                """, (semaine,))
                indicateurs = cur.fetchall()

        statuts = [row[4] for row in indicateurs]

        if "URGENCE" in statuts:
            situation_globale = "URGENCE"
        elif "ALERTE" in statuts:
            situation_globale = "ALERTE"
        else:
            situation_globale = "NORMAL"

        rapport = {
            "semaine": semaine,
            "region": "Occitanie",
            "code_region": "76",
            "date_generation": datetime.utcnow().isoformat(),
            "situation_globale": situation_globale,
            "nb_syndromes": len(indicateurs),
            "indicateurs": [
                {
                    "syndrome": row[0],
                    "valeur_ias": row[1],
                    "z_score": row[2],
                    "r0_estime": row[3],
                    "statut": row[4],
                }
                for row in indicateurs
            ],
        }

        path = f"/data/ars/rapports/{annee}/{num_sem}"
        os.makedirs(path, exist_ok=True)

        fichier = f"{path}/rapport_{semaine}.json"

        with open(fichier, "w", encoding="utf-8") as f:
            json.dump(rapport, f, indent=2, ensure_ascii=False)

        logger.info(f"RAPPORT_OK:{fichier}")
    except Exception:
        logger.exception("ERREUR_RAPPORT")
        raise



default_args = {
"owner": "ars-occitanie", "depends_on_past": False, "email_on_failure": True, "email_on_retry": False, "retries": 2,
"retry_delay": timedelta(minutes=5), "execution_timeout": timedelta(hours=2),
}

# DAG Principal
with DAG(
    dag_id="ars_epidemio_dag", 
    default_args=default_args,
    description="Pipeline surveillance épidémiologique ARS Occitanie", 
    schedule_interval="0 6 * * 1",
    start_date=datetime(2024, 4, 15),
    catchup=True, max_active_runs=1,
    tags=["sante-publique", "epidemio", "docker-compose"],
) as dag:


    test_connexions = PythonOperator(
        task_id="verifier_connexions",
        python_callable=verifier_connexions
    )

    init_base_donnees = PostgresOperator(
        task_id="init_base_donnees",
        postgres_conn_id="postgres_ars",
        sql="sql/init_ars_epidemio.sql",
        autocommit=True
    )

    collecte_sursaud = PythonOperator(
        task_id="collecter_donnees_sursaud",
        python_callable=collecter_donnees_ias
    )

    archiver = PythonOperator(
        task_id="archiver_local",
        python_callable=archiver_local
    )

    verifier = PythonOperator(
        task_id="verifier_archive",
        python_callable=verifier_archive
    )

    calcul_indicateurs = PythonOperator(
        task_id="calculer_indicateurs_epidemiques",
        python_callable=calculer_indicateurs_epidemiques
    )

    inserer_postgres = PythonOperator(
        task_id="inserer_donnees_postgres",
        python_callable=inserer_donnees_postgres
    )

    evaluer = BranchPythonOperator(
        task_id="evaluer_situation_epidemique",
        python_callable=evaluer_situation_epidemique
    )

    alerte_ars = PythonOperator(
        task_id="declencher_alerte_ars",
        python_callable=declencher_alerte_ars
    )

    bulletin = PythonOperator(
        task_id="envoyer_bulletin_surveillance",
        python_callable=envoyer_bulletin_surveillance
    )

    normale = PythonOperator(
        task_id="confirmer_situation_normale",
        python_callable=confirmer_situation_normale
    )

    generer_rapport = PythonOperator(
        task_id="generer_rapport_hebdomadaire",
        python_callable=generer_rapport_hebdomadaire,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    test_connexions >> init_base_donnees >> collecte_sursaud >> archiver >> verifier >> calcul_indicateurs >> inserer_postgres >> evaluer

    evaluer >> [alerte_ars, bulletin, normale]

    [alerte_ars, bulletin, normale] >> generer_rapport 