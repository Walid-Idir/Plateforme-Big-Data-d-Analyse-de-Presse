# Rapport Technique — Architecture de données
## Plateforme Big Data pour la collecte et l'analyse d'articles de presse

**Filière :** IADATA  
**Établissement :** EMSI — Casablanca  
**Année académique :** 2025/2026

---

## 1. Contexte et objectifs

Les médias numériques publient des milliers d'articles chaque jour. L'exploitation automatique de ces données présente un intérêt majeur pour :

- **Identifier les tendances d'actualité** : quels sujets dominent l'espace médiatique ?
- **Analyser les thèmes dominants** : segmentation thématique par catégorie, langue, région.
- **Suivre les événements en temps réel** : streaming d'articles dès leur publication.
- **Détecter les fake news** : corrélation de sources multiples et analyse de sentiment.

Ce rapport décrit l'architecture, les choix technologiques, l'implémentation et les résultats obtenus pour la plateforme Big Data développée dans le cadre de ce projet.

---

## 2. Architecture globale

La solution adopte une **architecture en couches** combinant :

1. **Ingestion** (batch + streaming)
2. **Data Lake** (MinIO — S3-compatible)
3. **Médaillon** (Bronze / Silver / Gold)
4. **Transformation** ETL/ELT (Python)
5. **Orchestration** (Apache Airflow)
6. **Data Warehouse** (PostgreSQL)
7. **Qualité** (contrôles sur 3 dimensions)
8. **Gouvernance** (catalogue + lineage)
9. **Visualisation** (Metabase)

```
Sources Web
    │
    ▼ [Scraper Python — BeautifulSoup]
    │
    ├──► Batch (Airflow @hourly) ────────────┐
    │                                         │
    └──► Streaming (Kafka topic: news) ──────┘
                                              │
                                              ▼
                                    ┌─────────────────┐
                                    │   Data Lake     │
                                    │   (MinIO S3)    │
                                    │                 │
                                    │  /bronze  (raw) │
                                    │  /silver  (DQ)  │
                                    │  /gold    (BI)  │
                                    └────────┬────────┘
                                             │
                            ┌────────────────┼────────────────┐
                            │                │                │
                            ▼                ▼                ▼
                       DQ Checks      Médaillon ETL     Gouvernance
                       (3 dims)    Bronze→Silver→Gold    (Catalog)
                            │                │                │
                            └────────────────┼────────────────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │  PostgreSQL DW  │
                                    │  (Fact Tables)  │
                                    └────────┬────────┘
                                             │
                                             ▼
                                    ┌─────────────────┐
                                    │   Metabase      │
                                    │  (Dashboards)   │
                                    └─────────────────┘
```

Tous les services sont containerisés via **Docker Compose** pour garantir la reproductibilité et la portabilité.

---

## 3. Sources de données

### 3.1 Sites d'actualité marocains

| Source | URL | Langue | Sélecteur d'articles |
|--------|-----|--------|----------------------|
| Hespress | hespress.com | Arabe | `h2.entry-title a` |
| Akhbarona | akhbarona.com | Arabe | `article a` |
| Lakom | lakom.com | Arabe | `h2 a` |
| Barlamane | barlamane.com | Arabe | `h2.entry-title a` |

### 3.2 Sites d'actualité internationaux

| Source | URL | Langue | Sélecteur d'articles |
|--------|-----|--------|----------------------|
| BBC News | bbc.com/news | Anglais | `a.gs-c-promo-heading` |
| CNN | cnn.com/world | Anglais | `h3.cd__headline a` |
| Reuters | reuters.com | Anglais | `article a` |
| Al Jazeera | aljazeera.com | Anglais/Arabe | `a.u-clickable-card__link` |

### 3.3 Champs collectés

Pour chaque article, le scraper extrait : titre, auteur, date, contenu, source, URL, catégorie, et horodatage de collecte.

---

## 4. Ingestion

### 4.1 Batch Ingestion

Le scraper Python (`src/scraper/scraper.py`) est déclenché **toutes les heures** par le DAG Airflow `news_batch_pipeline`.

**Flux :**
1. Récupération de la page de liste du site (`list_url`)
2. Extraction des liens d'articles (`discover_article_links`)
3. Téléchargement de chaque article (max 20 par site par run)
4. Sérialisation JSON → écriture dans `data/bronze/`
5. Upload optionnel vers MinIO (`bronze/`)

**Configuration YAML** (`src/scraper/config.yaml`) : chaque site est défini par ses sélecteurs CSS pour titre, auteur, date et contenu.

### 4.2 Streaming Ingestion

Un producteur Kafka (`src/streaming/kafka_producer.py`) lit les fichiers Bronze et publie chaque article comme **événement JSON** sur le topic `news`.

Un consommateur (`src/streaming/kafka_consumer.py`) consomme le topic, applique l'enrichissement NLP (mots-clés, entités) et persiste vers Silver et Gold.

**Infrastructure Kafka :**
- Zookeeper 3.9 (coordination)
- Kafka 3.6 (broker)
- Topic `news` (auto-créé)

---

## 5. Data Lake — MinIO

### 5.1 Choix technologique

**MinIO** a été sélectionné pour les raisons suivantes :
- Compatible API S3 (migration facile vers AWS S3, Azure Blob, GCS)
- Déploiement local via Docker (pas de dépendance cloud)
- Interface d'administration intégrée (console web)
- Support des policies de bucket (gouvernance)

### 5.2 Organisation des buckets

```
news-data/
  ├── bronze/   Articles bruts JSON (horodatés)
  ├── silver/   Articles nettoyés et enrichis
  └── gold/     Tables analytiques JSON
```

### 5.3 Accès

- **Console Web :** http://localhost:9001
- **API S3 :** http://localhost:9000
- **Credentials :** minioadmin / minioadmin (à sécuriser en production)

---

## 6. Architecture Médaillon

### 6.1 Couche Bronze 🥉

**Données :** Articles bruts tels que scraped.  
**Format :** JSON, un fichier par article, nommé `YYYYMMDD_HHMMSS_<uuid>.json`  
**Politique :** Aucune transformation ; conservation 90 jours.

### 6.2 Couche Silver 🥈

**Transformations appliquées :**

| Transformation | Outil | Description |
|----------------|-------|-------------|
| Suppression HTML | BeautifulSoup | Nettoyage des balises résiduelles |
| Normalisation espaces | Python `re` | Remplacement des séquences d'espaces |
| Détection de langue | `langdetect` | ISO 639-1 (en, fr, ar, …) |
| Normalisation dates | `python-dateutil` | Conversion en ISO 8601 |
| Comptage de mots | Python | `word_count` sur `content_clean` |

Implémenté dans `src/transform/medallion.py` (fonction `bronze_to_silver`).

### 6.3 Couche Gold 🥇

**Tables analytiques produites :**

| Table | Granularité | Utilisation |
|-------|-------------|-------------|
| `articles_per_source` | Source | Volume par média |
| `articles_per_day` | Jour | Tendance quotidienne |
| `articles_per_category` | Catégorie | Thèmes dominants |
| `articles_per_language` | Langue | Distribution linguistique |
| `top_keywords` | Mot-clé | Fréquence d'apparition |
| `daily_trends` | Jour × Source | Tendances croisées |

---

## 7. Transformation ETL/ELT

### 7.1 Pipeline Médaillon (ELT)

```bash
# Bronze → Silver → Gold en une commande
python src/transform/medallion.py full \
  --bronze data/bronze \
  --silver data/silver \
  --gold   data/gold
```

La logique est **ELT** : les données brutes sont d'abord chargées (Bronze), puis transformées en place (Silver), puis agrégées (Gold).

### 7.2 Chargement DW (ETL)

`src/etl/load_to_dw.py` charge :
1. Les articles individuels (`articles_gold`) avec déduplication par URL
2. Les tables de faits (`fact_articles_per_source`, `fact_top_keywords`, etc.)
3. Le journal DQ (`dq_audit_log`)

---

## 8. Orchestration — Apache Airflow

### 8.1 DAG Batch : `news_batch_pipeline`

**Planification :** `@hourly`

```
scrape_news
    │
    ▼
dq_bronze ──────────────── (trigger_rule=ALL_DONE)
    │
    ▼
bronze_to_silver
    │
    ▼
dq_silver ──────────────── (trigger_rule=ALL_DONE)
    │
    ▼
silver_to_gold
    │
    ▼
load_dw
    │
    ▼
update_catalog ─────────── (trigger_rule=ALL_DONE)
```

**Stratégie de résilience :**
- Retries automatiques (2 tentatives, délai 3 min)
- `trigger_rule=ALL_DONE` sur les tâches DQ pour qu'elles s'exécutent même si la tâche précédente échoue partiellement

### 8.2 DAG Streaming : `news_streaming_pipeline`

**Planification :** Manuel  
**Tâches :** `produce_to_kafka` → `consume_and_enrich`

---

## 9. Data Warehouse — PostgreSQL

### 9.1 Modèle de données

Le DW adopte un **schéma en étoile** simplifié :

```
                ┌─────────────────────┐
                │   articles_gold     │ ← Table de faits principale
                │   (dim + mesures)   │
                └─────────────────────┘

  ┌──────────────────┐  ┌───────────────────────┐  ┌────────────────────┐
  │ fact_per_source  │  │   fact_per_day        │  │ fact_top_keywords  │
  │ fact_per_category│  │   fact_daily_trends   │  │ dq_audit_log      │
  └──────────────────┘  └───────────────────────┘  └────────────────────┘
```

### 9.2 Vues analytiques

```sql
vw_articles_by_source    -- GROUP BY source
vw_articles_by_day       -- GROUP BY DATE(published_at)
vw_articles_by_language  -- GROUP BY language
vw_articles_by_category  -- GROUP BY category
```

---

## 10. Qualité des données

### 10.1 Trois dimensions de qualité

#### Complétude
- Article sans titre → `missing_title`
- Article sans contenu → `missing_content`
- Date absente → `missing_date`
- Source manquante → `missing_source`
- URL vide → `missing_url`

#### Cohérence
- URL sans schéma HTTP → `invalid_url_scheme`
- Date non parseable → `unparseable_date_field_*`
- Source numérique → `source_is_numeric`

#### Validité
- Contenu < 50 caractères → `content_too_short`
- Titre > 500 caractères → `title_too_long`
- HTML résiduel dans contenu nettoyé → `html_in_cleaned_content`
- Contenu > 500 000 caractères → `content_suspiciously_long`

### 10.2 Rapport DQ

```json
{
  "total_articles": 142,
  "articles_with_issues": 18,
  "pass_rate": 87.32,
  "dimension_failure_counts": {
    "completeness": 12,
    "consistency": 3,
    "validity": 5
  }
}
```

Le rapport est enregistré dans `data/reports/` et inséré dans `dq_audit_log` lors du chargement DW.

---

## 11. Gouvernance des données

### 11.1 Data Catalog

Le catalogue (`src/governance/catalog.py`) documente automatiquement :

- **Description** de chaque couche (Bronze/Silver/Gold)
- **Schéma** des champs (data dictionary)
- **Statistiques** : nombre de fichiers, taille
- **Sources** : 8 sites avec langue et pays
- **Lineage** : scraper → Bronze → Silver → Gold → DW
- **Politiques** : rétention, PII, accès

Sortie : `data/catalog.json` + `data/catalog.md` (lisible par humain)

### 11.2 Traçabilité (Lineage)

```
Scraper (scraper.py)
  └─► Bronze JSON (data/bronze/)
       └─► [medallion.py bronze_to_silver]
            └─► Silver JSON (data/silver/)
                 ├─► [medallion.py build_gold]
                 │    └─► Gold JSON (data/gold/)
                 │         └─► [load_to_dw.py] PostgreSQL
                 └─► [dq_report.py] DQ Audit Log
```

---

## 12. Visualisation — Metabase

Metabase est connecté à PostgreSQL et permet de créer des dashboards interactifs :

| Dashboard | Source de données | KPIs |
|-----------|-------------------|------|
| Tendances d'actualité | `vw_articles_by_day` | Articles/jour, variation % |
| Répartition par source | `vw_articles_by_source` | Barres + camembert |
| Mots-clés | `fact_top_keywords` | Top 20, nuage de mots |
| Distribution linguistique | `vw_articles_by_language` | AR/EN/FR/… |
| Thèmes dominants | `vw_articles_by_category` | Top catégories |

---

## 13. Infrastructure Docker

### 13.1 Services déployés

| Service | Image | Port | Rôle |
|---------|-------|------|------|
| `minio` | minio/minio | 9000, 9001 | Data Lake |
| `minio_init` | minio/mc | — | Création des buckets |
| `postgres` | postgres:16 | 5432 | Data Warehouse |
| `zookeeper` | bitnami/zookeeper:3.9 | 2181 | Coordination Kafka |
| `kafka` | bitnami/kafka:3.6 | 9092 | Message broker |
| `scraper` | (build local) | — | Collecte batch |
| `airflow-init` | apache/airflow:2.8 | — | Initialisation Airflow |
| `airflow-webserver` | apache/airflow:2.8 | 8080 | Interface Airflow |
| `airflow-scheduler` | apache/airflow:2.8 | — | Planificateur Airflow |
| `metabase` | metabase/metabase | 3000 | Dashboards BI |

### 13.2 Démarrage

```bash
docker-compose up -d
docker-compose ps        # vérification
docker-compose logs -f   # logs en direct
```

---

## 14. Tests

### Tests unitaires

```bash
pytest -q tests/
```

Tests couverts :
- Parsing HTML avec données d'exemple (`tests/sample_bbc.html`, `tests/sample_hespress.html`)
- Contrôles DQ basiques (articles valides/invalides)

### CI/CD

Workflow GitHub Actions (`.github/workflows/ci.yml`) :
- Déclenchement sur push `main` et PR
- Python 3.11 · install deps · pytest

---

## 15. Difficultés rencontrées et solutions

| Problème | Solution |
|----------|----------|
| Anti-scraping (rate limiting) | User-Agent Mozilla, délai entre requêtes |
| Sites à rendu JavaScript | Limitation documentée ; Selenium/Playwright optionnel |
| Encodage mixte (AR/EN) | `ensure_ascii=False` partout, UTF-8 forcé |
| Dates multi-formats | `python-dateutil` avec fallback gracieux |
| Airflow connexion PostgreSQL | Partage du service `postgres` comme métastore ET DW |
| Kafka démarrage lent | `healthcheck` + retry dans Docker Compose |

---

## 16. Conclusion et perspectives

### Réalisations

✅ Scraper configurable (8 sources, 2 langues)  
✅ Ingestion batch (Airflow hourly) + streaming (Kafka)  
✅ Data Lake MinIO avec 3 couches (bronze/silver/gold)  
✅ Architecture Médaillon complète  
✅ ETL Python : nettoyage, détection de langue, agrégations  
✅ Data Warehouse PostgreSQL (6 tables de faits + 4 vues)  
✅ Qualité des données (3 dimensions, rapport JSON)  
✅ Gouvernance : Data Catalog + Lineage  
✅ Visualisation : Metabase connecté au DW  
✅ Orchestration : DAG Airflow 7 étapes  
✅ Infrastructure Docker Compose (10 services)  
✅ CI/CD GitHub Actions  

### Évolutions possibles

1. **Spark** pour le traitement distribué à grande échelle (remplacement de la transformation Python)
2. **dbt** pour les transformations SQL déclaratives au niveau DW
3. **Great Expectations** pour des tests de qualité plus avancés
4. **Grafana + Prometheus** pour le monitoring de l'infrastructure
5. **Scrapy** pour un scraping plus robuste et parallèle
6. **Analyse NLP avancée** : détection de sentiment, résumé automatique, topic modeling (LDA)
7. **Détection de fake news** : modèle ML entraîné sur sources fiables vs non-fiables

---

*Rapport généré le 2026-05-05 — EMSI Casablanca, Filière IADATA*
