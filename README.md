# 📰 News Media Big Data Platform

> **Plateforme Big Data** pour la collecte, le stockage, la transformation et l'analyse automatique d'articles de presse en temps réel.

[![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)](https://python.org)
[![Apache Airflow](https://img.shields.io/badge/Airflow-2.8-red?logo=apache-airflow)](https://airflow.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Kafka-3.6-black?logo=apache-kafka)](https://kafka.apache.org)
[![MinIO](https://img.shields.io/badge/MinIO-DataLake-orange?logo=minio)](https://min.io)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?logo=postgresql)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-blue?logo=docker)](https://docker.com)

---

## 🗂️ Table des matières

1. [Architecture](#architecture)
2. [Structure du projet](#structure-du-projet)
3. [Démarrage rapide](#démarrage-rapide)
4. [Sources de données](#sources-de-données)
5. [Architecture Médaillon](#architecture-médaillon)
6. [Ingestion Batch & Streaming](#ingestion-batch--streaming)
7. [Data Lake — MinIO](#data-lake--minio)
8. [Transformation ETL](#transformation-etl)
9. [Orchestration — Airflow](#orchestration--airflow)
10. [Data Warehouse](#data-warehouse)
11. [Qualité des données](#qualité-des-données)
12. [Gouvernance](#gouvernance)
13. [Visualisation — Metabase](#visualisation--metabase)
14. [Tests](#tests)
15. [CI/CD](#cicd)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     NEWS MEDIA BIG DATA PLATFORM                    │
│                                                                     │
│  ┌──────────┐   Batch    ┌──────────┐   Bronze   ┌──────────────┐  │
│  │  News    │──────────▶│ Scraper  │──────────▶ │  Data Lake   │  │
│  │  Sites   │           │ (Python) │            │  (MinIO/S3)  │  │
│  │ BBC/CNN/ │  Stream   │          │            │              │  │
│  │ Hespress │──────────▶│  Kafka   │            │ bronze/      │  │
│  └──────────┘           └──────────┘            │ silver/      │  │
│                                                  │ gold/        │  │
│  ┌───────────────────────────────────────────┐   └──────────────┘  │
│  │          MEDALLION PIPELINE               │                      │
│  │  Bronze ──▶ Silver ──▶ Gold               │                      │
│  │  (raw)  (clean/NLP)  (aggregations)       │                      │
│  └───────────────────────────────────────────┘                      │
│                                                                     │
│  ┌──────────────┐   ETL   ┌──────────────┐   BI  ┌─────────────┐  │
│  │  Airflow     │────────▶│  PostgreSQL  │──────▶│  Metabase   │  │
│  │ (Orchestrat.)│         │  (DW Gold)   │       │ (Dashboards)│  │
│  └──────────────┘         └──────────────┘       └─────────────┘  │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │        DATA QUALITY & GOVERNANCE                             │  │
│  │  DQ Report (completeness/consistency/validity) · Catalog     │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Structure du projet

```
Project Scrapping/
│
├── src/
│   ├── scraper/
│   │   ├── scraper.py          # Web scraper (BeautifulSoup)
│   │   └── config.yaml         # Configuration des 8 sites
│   │
│   ├── transform/
│   │   ├── clean.py            # Nettoyage de base
│   │   └── medallion.py        # Pipeline Bronze→Silver→Gold
│   │
│   ├── etl/
│   │   └── load_to_dw.py       # ETL Gold → PostgreSQL DW
│   │
│   ├── streaming/
│   │   ├── kafka_producer.py   # Publie articles vers Kafka
│   │   └── kafka_consumer.py   # Consomme + enrichit via NLP
│   │
│   ├── quality/
│   │   ├── dq_checks.py        # Contrôles basiques
│   │   └── dq_report.py        # Rapport DQ complet (3 dimensions)
│   │
│   ├── governance/
│   │   └── catalog.py          # Data Catalog & Lineage
│   │
│   └── enrich/
│       └── nlp.py              # NLP: keywords, entities
│
├── airflow/
│   └── dags/
│       ├── news_pipeline.py        # DAG batch (toutes les heures)
│       └── streaming_pipeline.py   # DAG streaming (manuel)
│
├── sql/
│   └── schema_dw.sql           # Schéma complet DW + vues analytiques
│
├── tests/
│   └── test_scraper.py         # Tests unitaires
│
├── scripts/
│   ├── run_tests.sh
│   └── init_minio.sh
│
├── docker-compose.yml          # Tous les services
├── Dockerfile                  # Image scraper/ETL
├── requirements.txt
├── .env.example
├── README.md
└── REPORT.md
```

---

## Démarrage rapide

### Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) ≥ 24
- [Python](https://python.org) 3.11+ (pour exécution locale)

### 1. Cloner et configurer l'environnement

```bash
# Copier le fichier de variables d'environnement
cp .env.example .env
```

### 2. Démarrer toute l'infrastructure

```bash
docker-compose up -d
```

Cela démarre **8 services** :

| Service | URL | Description |
|---------|-----|-------------|
| MinIO Console | http://localhost:9001 | Data Lake UI (admin/minioadmin) |
| MinIO API | http://localhost:9000 | S3-compatible API |
| PostgreSQL | localhost:5432 | Data Warehouse |
| Airflow | http://localhost:8080 | Orchestration (admin/admin) |
| Metabase | http://localhost:3000 | Tableaux de bord BI |
| Kafka | localhost:9092 | Message broker |
| Zookeeper | localhost:2181 | Kafka coordination |

### 3. Vérifier les services

```bash
docker-compose ps
```

### 4. Environnement Python local (optionnel)

```bash
python -m venv .venv
# Windows:
.venv\Scripts\Activate.ps1
# Linux/Mac:
source .venv/bin/activate

pip install -r requirements.txt
```

---

## Sources de données

| Source | URL | Langue | Région |
|--------|-----|--------|--------|
| BBC News | https://www.bbc.com/news | EN | International |
| CNN | https://www.cnn.com/world | EN | USA |
| Reuters | https://www.reuters.com | EN | International |
| Al Jazeera | https://www.aljazeera.com | EN/AR | Qatar |
| Hespress | https://www.hespress.com | AR | Maroc |
| Akhbarona | https://akhbarona.com | AR | Maroc |
| Lakom | https://lakom.com | AR | Maroc |
| Barlamane | https://barlamane.com | AR | Maroc |

### Données collectées

| Champ | Type | Description |
|-------|------|-------------|
| `title` | string | Titre de l'article |
| `author` | string | Auteur(s) |
| `date` | string | Date de publication (brut) |
| `content` | string | Corps de l'article (peut contenir HTML) |
| `source` | string | Nom du site source |
| `url` | string | URL canonique |
| `category` | string | Catégorie / rubrique |
| `collected_at` | datetime | Horodatage de collecte (UTC) |

---

## Architecture Médaillon

```
Bronze (Raw)  →  Silver (Clean)  →  Gold (Analytics)
   JSON             JSON              JSON + PostgreSQL
```

### 🥉 Bronze — Données brutes

```bash
# Lancer le scraper (écriture dans data/bronze/)
python src/scraper/scraper.py --config src/scraper/config.yaml --out data/bronze
```

### 🥈 Silver — Nettoyage & enrichissement

Transformations appliquées :
- ✅ Suppression des balises HTML (`BeautifulSoup`)
- ✅ Normalisation des espaces blancs
- ✅ Détection de langue (`langdetect`)
- ✅ Normalisation des dates (`python-dateutil`)
- ✅ Calcul du nombre de mots

```bash
python src/transform/medallion.py silver \
  --bronze data/bronze \
  --silver data/silver
```

### 🥇 Gold — Tables analytiques

Tables produites :

| Fichier | Description |
|---------|-------------|
| `articles_per_source.json` | Nombre d'articles par source |
| `articles_per_day.json` | Articles par jour |
| `articles_per_category.json` | Articles par catégorie |
| `articles_per_language.json` | Articles par langue détectée |
| `top_keywords.json` | Top 100 mots-clés |
| `daily_trends.json` | Tendances par source et par jour |
| `articles/` | Articles individuels enrichis |

```bash
python src/transform/medallion.py gold \
  --silver data/silver \
  --gold data/gold
```

**Pipeline complet en une commande :**

```bash
python src/transform/medallion.py full \
  --bronze data/bronze \
  --silver data/silver \
  --gold data/gold
```

---

## Ingestion Batch & Streaming

### Batch (toutes les heures)

Orchestré par Airflow DAG `news_batch_pipeline` (planifié `@hourly`).

```bash
# Déclencher manuellement via CLI Airflow
docker exec news_airflow_scheduler airflow dags trigger news_batch_pipeline
```

### Streaming (Kafka)

```bash
# 1. Produire des messages depuis les fichiers bronze
python src/streaming/kafka_producer.py

# 2. Consommer et enrichir en temps réel (dans un autre terminal)
python src/streaming/kafka_consumer.py
```

Le topic Kafka `news` reçoit chaque article comme événement JSON.

---

## Data Lake — MinIO

Les données brutes et transformées sont stockées dans MinIO :

```
news-data/
  bronze/   ← Articles bruts JSON
  silver/   ← Articles nettoyés JSON
  gold/     ← Tables analytiques JSON
```

**Accès via la console :** http://localhost:9001 (minioadmin / minioadmin)

**Via Python :**
```python
from minio import Minio
client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
```

---

## Transformation ETL

### Bronze → Silver → Gold (Pipeline Médaillon)

```bash
python src/transform/medallion.py full --bronze data/bronze --silver data/silver --gold data/gold
```

### Gold → Data Warehouse (PostgreSQL)

```bash
python src/etl/load_to_dw.py \
  --gold data/gold \
  --db-url postgresql://scrap_user:scrap_pass@localhost:5432/scrap_dw
```

---

## Orchestration — Airflow

**Interface :** http://localhost:8080 (admin / admin)

### DAGs disponibles

| DAG | Déclencheur | Description |
|-----|-------------|-------------|
| `news_batch_pipeline` | `@hourly` | Pipeline complet batch |
| `news_streaming_pipeline` | Manuel | Pipeline Kafka streaming |

### Pipeline batch (7 étapes)

```
scrape_news → dq_bronze → bronze_to_silver → dq_silver → silver_to_gold → load_dw → update_catalog
```

---

## Data Warehouse

### Schéma PostgreSQL

```sql
-- Tables principales
articles_gold          -- Articles individuels enrichis
fact_articles_per_day  -- Articles par jour
fact_articles_per_source
fact_articles_per_category
fact_articles_per_language
fact_top_keywords
fact_daily_trends
dq_audit_log           -- Journal des contrôles qualité

-- Vues analytiques
vw_articles_by_source
vw_articles_by_day
vw_articles_by_language
vw_articles_by_category
```

**Initialiser le schéma :**
```bash
docker exec -i news_postgres psql -U scrap_user -d scrap_dw < sql/schema_dw.sql
```

**Exemples de requêtes :**
```sql
-- Top sources
SELECT * FROM vw_articles_by_source LIMIT 10;

-- Tendances du jour
SELECT day, article_count FROM vw_articles_by_day ORDER BY day DESC LIMIT 30;

-- Mots-clés les plus fréquents
SELECT keyword, frequency FROM fact_top_keywords ORDER BY frequency DESC LIMIT 20;
```

---

## Qualité des données

### Trois dimensions couvertes

| Dimension | Tests |
|-----------|-------|
| **Complétude** | Titre manquant · Contenu vide · Date absente · Source manquante · URL vide |
| **Cohérence** | URL sans schéma HTTP · Date non parseable · Source numérique |
| **Validité** | Contenu trop court (<50 chars) · Titre trop long · HTML résiduel dans contenu nettoyé |

### Lancer un rapport DQ

```bash
# Sur les données Bronze
python src/quality/dq_report.py --input data/bronze --output data/reports/dq_bronze.json

# Sur les données Silver
python src/quality/dq_report.py --input data/silver --output data/reports/dq_silver.json
```

**Exemple de sortie :**
```
=== DQ Report ===
Total articles  : 142
With issues     : 18
Pass rate       : 87.32%
Dimension failures: {'completeness': 12, 'consistency': 3, 'validity': 5}
```

---

## Gouvernance

### Data Catalog

```bash
python src/governance/catalog.py \
  --data-dir data \
  --output data/catalog.json \
  --markdown data/catalog.md
```

Le catalogue documente automatiquement :
- Description de chaque couche (Bronze/Silver/Gold)
- Nombre de fichiers et taille
- Sources de données et leurs métadonnées
- Politiques de rétention et d'accès
- Lineage du pipeline

### Politiques

| Politique | Valeur |
|-----------|--------|
| Rétention Bronze | 90 jours |
| Rétention Silver/Gold | Indéfini |
| PII | Noms d'auteurs seulement (pas de données personnelles) |
| Accès | Politique de bucket MinIO — écriture réservée au service scraper |

---

## Visualisation — Metabase

**Accès :** http://localhost:3000

Connectez Metabase à la base PostgreSQL (`news_postgres:5432 / scrap_dw`) pour créer des dashboards :

### Dashboards recommandés

| Dashboard | Métriques |
|-----------|-----------|
| 📈 Tendances d'actualité | `vw_articles_by_day` — Volume quotidien |
| 🌍 Sources | `vw_articles_by_source` — Répartition par source |
| 🔑 Mots-clés | `fact_top_keywords` — Nuage de mots |
| 🌐 Langues | `vw_articles_by_language` — Distribution linguistique |
| 📂 Catégories | `vw_articles_by_category` — Thèmes dominants |

---

## Tests

```bash
# Environnement virtuel actif
pytest -q

# Avec rapport de couverture
pytest --cov=src tests/
```

---

## CI/CD

Un workflow GitHub Actions est défini dans `.github/workflows/ci.yml` :

- **Déclenchement :** push sur `main` ou Pull Request
- **Étapes :** install deps → lint (flake8) → pytest
- **Python :** 3.11

---

## 📝 Livrables

| Livrable | Fichier |
|----------|---------|
| Rapport technique | `REPORT.md` |
| README complet | `README.md` (ce fichier) |
| Dockerfile | `Dockerfile` |
| Docker Compose | `docker-compose.yml` |
| DAG batch Airflow | `airflow/dags/news_pipeline.py` |
| DAG streaming Airflow | `airflow/dags/streaming_pipeline.py` |
| Scraper configurable | `src/scraper/scraper.py` |
| Pipeline Médaillon | `src/transform/medallion.py` |
| ETL DW | `src/etl/load_to_dw.py` |
| Qualité données | `src/quality/dq_report.py` |
| Gouvernance | `src/governance/catalog.py` |
| Schéma DW SQL | `sql/schema_dw.sql` |

---

*EMSI Casablanca — IADATA 2025/2026*
