# TP Noté Airflow — Data Platform Santé Publique ARS Occitanie

## Auteur
- Nom : DRAPERI
- Prénom : Nicolas
- Formation : Mastère IA NIE
- Date : 13/04/2026

---

## Prérequis
- Docker Desktop >= 4.0
- Docker Compose >= 2.0
- Python >= 3.11 (optionnel pour tests locaux)

---

## Instructions de déploiement

### 1. Démarrage de la stack

docker-compose up -d

Accéder à Airflow :
http://localhost:8080

---

### 2. Configuration Airflow

Connexion PostgreSQL :

Conn Id : postgres_ars  
Type : Postgres  
Host : postgres-ars  
Port : 5432  
Schema : ars_epidemio  
Login : postgres  
Password : postgres  

---

Variables Airflow :
```
{
  "archive_base_path": "/data/ars",
  "departements_occitanie": ["09","11","12","30","31","32","34","46","48"],
  "syndromes_surveilles": ["GRIPPE","GEA"],
  "semaines_historique": 12,
  "seuil_alerte_zscore": 1.5,
  "seuil_urgence_zscore": 3.0
}
```

---

### 3. Démarrage du pipeline

```
docker compose exec airflow-webserver airflow dags backfill ars_epidemio_dag --start-date 2024-04-15 --end-date 2024-12-31
```

---

## Particularité du dataset

- Dataset incomplet  
- Données disponibles à partir de 2024-S14  
- Structure basée sur des saisons  

SEMAINE_FIXE = "2024-S14"

---

## Architecture du pipeline

1. Vérification connexions  
2. Init base PostgreSQL  
3. Collecte données  
4. Archivage  
5. Vérification  
6. Calcul indicateurs  
7. Insertion DB  
8. Évaluation  
9. Rapport  

---

## Stockage

/data/ars/raw/YYYY/SXX/  
/data/ars/rapports/YYYY/SXX/

---

## Base PostgreSQL

Tables principales :
- donnees_hebdomadaires : données IAS agrégées par semaine et syndrome
- indicateurs_epidemiques : indicateurs calculés (z-score, R0, statut)
- rapports_ars : rapports hebdomadaires consolidés

Gestion des doublons :
- Toutes les insertions utilisent ON CONFLICT DO UPDATE pour garantir l’idempotence du pipeline

---

## Difficultés

- Dataset incomplet  
- Colonnes saisonnières  
- Gestion semaines ISO  

---

## Conclusion

Pipeline complet :
ingestion → traitement → stockage → analyse → rapport

En raison du manque de data sur le dataset la récolte commence en avril 2024 et ne peut pas aller plus loin que 2025