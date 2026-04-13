import csv
import io
import json
import logging
import os
from datetime import datetime, date
from typing import Optional, List, Dict

import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATASETS_IAS = {
    "GRIPPE": "https://www.data.gouv.fr/api/1/datasets/r/35f46fbb-7a97-46b3-a93c-35a471033447",
    "GEA": "https://www.data.gouv.fr/api/1/datasets/r/6c415be9-4ebf-4af5-b0dc-9867bb1ec0e3",
}

COL_OCCITANIE = "Loc_Reg76"
COL_OCCITANIE_FALLBACK = ["Loc_Reg73", "Loc_Reg91"]


def get_semaine_iso(reference_date: Optional[date] = None) -> str:
    if reference_date is None:
        reference_date = date.today()
    year, week, _ = reference_date.isocalendar()
    return f"{year}-S{week:02d}"


def safe_float(value: Optional[str]) -> Optional[float]:
    if value in (None, "", "NA"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def telecharger_csv_ias(url: str) -> List[Dict]:
    logger.info(f"Téléchargement : {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()

    content = response.content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(content), delimiter=";")

    rows: List[Dict] = []
    for row in reader:
        cleaned: Dict = {}
        for k, v in row.items():
            if v in ("NA", "", None):
                cleaned[k] = None
            else:
                cleaned[k] = v.replace(",", ".").strip()
        rows.append(cleaned)

    logger.info(f"{len(rows)} lignes récupérées")
    if rows:
        logger.info(f"Colonnes détectées : {list(rows[0].keys())}")
    return rows


def filtrer_semaine(rows: List[Dict], semaine: str) -> List[Dict]:
    annee_cible = int(semaine[:4])
    num_sem_cible = int(semaine.split("-S")[1])

    filtered: List[Dict] = []

    for row in rows:
        periode = row.get("PERIODE")
        if not periode:
            continue

        try:
            d = datetime.strptime(periode.strip(), "%d-%m-%Y").date()
        except ValueError:
            continue

        iso_year, iso_week, _ = d.isocalendar()
        if iso_year == annee_cible and iso_week == num_sem_cible:
            filtered.append(row)

    logger.info(f"{len(filtered)} jours pour la semaine {semaine}")
    return filtered


def extraire_valeur_occitanie(row: Dict) -> Optional[float]:
    # Code regionnal actuel pour Occitanie
    val_76 = safe_float(row.get(COL_OCCITANIE))
    if val_76 is not None:
        return val_76

    # Fallback : anciens codes régionaux correspondant à l’Occitanie
    fallback_values: List[float] = []
    for col in COL_OCCITANIE_FALLBACK:
        val = safe_float(row.get(col))
        if val is not None:
            fallback_values.append(val)

    if not fallback_values:
        return None

    return round(sum(fallback_values) / len(fallback_values), 3)


def agreger_semaine(rows: List[Dict], syndrome: str, semaine: str) -> Dict:
    valeurs_ias: List[float] = []
    min_saison_vals: List[float] = []
    max_saison_vals: List[float] = []

    for row in rows:
        valeur_occitanie = extraire_valeur_occitanie(row)
        if valeur_occitanie is not None:
            valeurs_ias.append(valeur_occitanie)

        min_val = safe_float(row.get("MIN_Saison"))
        if min_val is not None:
            min_saison_vals.append(min_val)

        max_val = safe_float(row.get("MAX_Saison"))
        if max_val is not None:
            max_saison_vals.append(max_val)

    def safe_mean(values: List[float]) -> Optional[float]:
        return round(sum(values) / len(values), 3) if values else None

    return {
        "semaine": semaine,
        "syndrome": syndrome,
        "valeur_ias": safe_mean(valeurs_ias),
        "seuil_min": safe_mean(min_saison_vals),
        "seuil_max": safe_mean(max_saison_vals),
        "nb_jours": len(valeurs_ias),
        "historique": {
            "Sais_2023_2024": safe_float(rows[0].get("Sais_2023_2024")) if rows else None,
            "Sais_2022_2023": safe_float(rows[0].get("Sais_2022_2023")) if rows else None,
            "Sais_2021_2022": safe_float(rows[0].get("Sais_2021_2022")) if rows else None,
            "Sais_2020_2021": safe_float(rows[0].get("Sais_2020_2021")) if rows else None,
            "Sais_2019_2020": safe_float(rows[0].get("Sais_2019_2020")) if rows else None,
        },
    }


def sauvegarder_donnees(donnees: Dict, semaine: str, output_dir: str) -> str:
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"ias_{semaine}.json")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(
            {
                "semaine": semaine,
                "collecte_le": datetime.utcnow().isoformat(),
                "source": "IAS OpenHealth / data.gouv.fr",
                "colonne_cible": COL_OCCITANIE,
                "fallback_regions": COL_OCCITANIE_FALLBACK,
                "syndromes": donnees,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )

    logger.info(f"Données sauvegardées : {output_path}")
    return output_path


if __name__ == "__main__":
    semaine = os.environ.get("SEMAINE_CIBLE", get_semaine_iso())
    output_dir = os.environ.get("OUTPUT_DIR", "/data/ars/raw")

    resultats: Dict = {}
    for syndrome, url in DATASETS_IAS.items():
        rows_all = telecharger_csv_ias(url)
        rows_sem = filtrer_semaine(rows_all, semaine)
        resultats[syndrome] = agreger_semaine(rows_sem, syndrome, semaine)

    chemin = sauvegarder_donnees(resultats, semaine, output_dir)
    print(f"COLLECTE_OK:{chemin}")