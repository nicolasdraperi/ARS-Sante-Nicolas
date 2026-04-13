import json
import logging
from typing import Optional, List

import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def calculer_zscore(valeur_actuelle: float, historique: List[float]) -> Optional[float]:
    """
    Calcule le z-score de la valeur IAS par rapport aux saisons historiques.
    Requiert au minimum 3 valeurs historiques.
    """
    valeurs_valides = [v for v in historique if v is not None]

    if len(valeurs_valides) < 3:
        logger.warning(f"Historique insuffisant ({len(valeurs_valides)} saisons)")
        return None

    moyenne = np.mean(valeurs_valides)
    ecart_type = np.std(valeurs_valides, ddof=1)

    if ecart_type == 0:
        return 0.0

    return float((valeur_actuelle - moyenne) / ecart_type)


def classifier_statut_ias(
    valeur_ias: float,
    seuil_min: Optional[float],
    seuil_max: Optional[float],
) -> str:
    """
    Classification selon les seuils IAS du dataset.
    """
    if seuil_max is not None and valeur_ias >= seuil_max:
        return "URGENCE"
    if seuil_min is not None and valeur_ias >= seuil_min:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_zscore(
    z_score: Optional[float],
    seuil_alerte_z: float = 1.5,
    seuil_urgence_z: float = 3.0,
) -> str:
    """
    Classification selon le z-score.
    """
    if z_score is None:
        return "NORMAL"
    if z_score >= seuil_urgence_z:
        return "URGENCE"
    if z_score >= seuil_alerte_z:
        return "ALERTE"
    return "NORMAL"


def classifier_statut_final(statut_ias: str, statut_zscore: str) -> str:
    """
    Retient le niveau le plus sévère entre IAS et z-score.
    """
    if "URGENCE" in (statut_ias, statut_zscore):
        return "URGENCE"
    if "ALERTE" in (statut_ias, statut_zscore):
        return "ALERTE"
    return "NORMAL"


def calculer_r0_simplifie(
    series_hebdomadaire: List[float],
    duree_infectieuse: int = 5,
) -> Optional[float]:
    """
    Estimation simplifiée du R0 à partir du taux de croissance moyen.
    """
    series_valides = [v for v in series_hebdomadaire if v is not None and v > 0]

    if len(series_valides) < 2:
        return None

    croissances = [
        (series_valides[i] - series_valides[i - 1]) / series_valides[i - 1]
        for i in range(1, len(series_valides))
        if series_valides[i - 1] > 0
    ]

    if not croissances:
        return None

    return max(0.0, float(1 + np.mean(croissances) * (duree_infectieuse / 7)))


def charger_json_archive(chemin_json: str) -> dict:
    with open(chemin_json, "r", encoding="utf-8") as f:
        return json.load(f)


def get_conn():
    hook = PostgresHook(postgres_conn_id="postgres_ars")
    return hook.get_conn()


def recuperer_series_precedentes(conn, syndrome: str, semaine_courante: str, limite: int = 12) -> List[float]:
    """
    Récupère les dernières valeurs IAS hebdomadaires déjà stockées en base
    pour estimer un R0 simplifié.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT valeur_ias
            FROM donnees_hebdomadaires
            WHERE syndrome = %s
              AND semaine <> %s
              AND valeur_ias IS NOT NULL
            ORDER BY annee DESC, numero_semaine DESC
            LIMIT %s
            """,
            (syndrome, semaine_courante, limite),
        )
        rows = cur.fetchall()

    valeurs = [float(r[0]) for r in rows if r[0] is not None]
    valeurs.reverse()
    return valeurs


def recuperer_duree_infectieuse(conn, syndrome: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT duree_infectieuse_jours
            FROM syndromes
            WHERE code = %s
            """,
            (syndrome,),
        )
        row = cur.fetchone()

    if not row or row[0] is None:
        return 5

    return int(row[0])


def inserer_indicateur(
    conn,
    semaine: str,
    syndrome: str,
    valeur_ias: Optional[float],
    z_score: Optional[float],
    r0_estime: Optional[float],
    nb_saisons_reference: int,
    statut: str,
    statut_ias: str,
    statut_zscore: str,
    commentaire: str,
):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO indicateurs_epidemiques (
                semaine,
                syndrome,
                valeur_ias,
                z_score,
                r0_estime,
                nb_saisons_reference,
                statut,
                statut_ias,
                statut_zscore,
                commentaire
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (semaine, syndrome)
            DO UPDATE SET
                valeur_ias = EXCLUDED.valeur_ias,
                z_score = EXCLUDED.z_score,
                r0_estime = EXCLUDED.r0_estime,
                nb_saisons_reference = EXCLUDED.nb_saisons_reference,
                statut = EXCLUDED.statut,
                statut_ias = EXCLUDED.statut_ias,
                statut_zscore = EXCLUDED.statut_zscore,
                commentaire = EXCLUDED.commentaire,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                semaine,
                syndrome,
                valeur_ias,
                z_score,
                r0_estime,
                nb_saisons_reference,
                statut,
                statut_ias,
                statut_zscore,
                commentaire,
            ),
        )
    conn.commit()


def calculer_indicateurs_depuis_json(chemin_json: str) -> str:
    data = charger_json_archive(chemin_json)
    semaine = data["semaine"]
    syndromes_data = data["syndromes"]

    conn = get_conn()

    try:
        for syndrome, infos in syndromes_data.items():
            valeur_ias = infos.get("valeur_ias")
            seuil_min = infos.get("seuil_min")
            seuil_max = infos.get("seuil_max")

            historique_dict = infos.get("historique", {})
            historique = list(historique_dict.values()) if historique_dict else []
            nb_saisons_reference = len([v for v in historique if v is not None])

            if valeur_ias is None:
                logger.warning(f"Aucune valeur IAS pour {syndrome} sur {semaine}")
                continue

            z_score = calculer_zscore(valeur_ias, historique)
            statut_ias = classifier_statut_ias(valeur_ias, seuil_min, seuil_max)
            statut_zscore = classifier_statut_zscore(z_score)

            duree_infectieuse = recuperer_duree_infectieuse(conn, syndrome)
            serie_precedente = recuperer_series_precedentes(conn, syndrome, semaine, limite=12)
            serie_complete = serie_precedente + [valeur_ias]
            r0_estime = calculer_r0_simplifie(serie_complete, duree_infectieuse)

            statut = classifier_statut_final(statut_ias, statut_zscore)

            commentaire = (
                f"Calcul automatique IAS. "
                f"Valeur={valeur_ias}, seuil_min={seuil_min}, seuil_max={seuil_max}, "
                f"z_score={round(z_score, 3) if z_score is not None else None}, "
                f"r0={round(r0_estime, 3) if r0_estime is not None else None}"
            )

            inserer_indicateur(
                conn=conn,
                semaine=semaine,
                syndrome=syndrome,
                valeur_ias=valeur_ias,
                z_score=round(z_score, 3) if z_score is not None else None,
                r0_estime=round(r0_estime, 3) if r0_estime is not None else None,
                nb_saisons_reference=nb_saisons_reference,
                statut=statut,
                statut_ias=statut_ias,
                statut_zscore=statut_zscore,
                commentaire=commentaire,
            )

            logger.info(f"Indicateur calculé pour {syndrome} / {semaine} : {statut}")

    finally:
        conn.close()

    return f"INDICATEURS_OK:{semaine}"