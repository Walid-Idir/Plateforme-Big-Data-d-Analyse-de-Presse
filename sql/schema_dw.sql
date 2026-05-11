-- =============================================================================
-- Data Warehouse Schema — News Media Big Data Platform
-- =============================================================================

-- -----------------------------------------------------------------------------
-- Core gold table: individual enriched articles
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS articles_gold (
    id              SERIAL PRIMARY KEY,
    title           TEXT,
    author          TEXT,
    published_at    TIMESTAMP WITH TIME ZONE,
    category        TEXT,
    content         TEXT,
    source          TEXT,
    url             TEXT UNIQUE,
    language        TEXT,
    word_count      INTEGER,
    collected_at    TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_articles_gold_published_at ON articles_gold (published_at);
CREATE INDEX IF NOT EXISTS idx_articles_gold_source       ON articles_gold (source);
CREATE INDEX IF NOT EXISTS idx_articles_gold_language     ON articles_gold (language);
CREATE INDEX IF NOT EXISTS idx_articles_gold_category     ON articles_gold (category);

-- -----------------------------------------------------------------------------
-- Analytical table: articles per day
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_articles_per_day (
    id          SERIAL PRIMARY KEY,
    day         DATE NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    updated_at  TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (day)
);

-- -----------------------------------------------------------------------------
-- Analytical table: articles per source
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_articles_per_source (
    id            SERIAL PRIMARY KEY,
    source        TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (source)
);

-- -----------------------------------------------------------------------------
-- Analytical table: articles per category
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_articles_per_category (
    id            SERIAL PRIMARY KEY,
    category      TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (category)
);

-- -----------------------------------------------------------------------------
-- Analytical table: articles per language
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_articles_per_language (
    id            SERIAL PRIMARY KEY,
    language      TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (language)
);

-- -----------------------------------------------------------------------------
-- Analytical table: top keywords (refreshed each run)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_top_keywords (
    id        SERIAL PRIMARY KEY,
    keyword   TEXT NOT NULL,
    frequency INTEGER NOT NULL DEFAULT 0,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (keyword)
);

-- -----------------------------------------------------------------------------
-- Analytical table: daily trend per source
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_daily_trends (
    id            SERIAL PRIMARY KEY,
    day           DATE NOT NULL,
    source        TEXT NOT NULL,
    article_count INTEGER NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE (day, source)
);

-- -----------------------------------------------------------------------------
-- Data quality audit log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_audit_log (
    id            SERIAL PRIMARY KEY,
    checked_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    layer         TEXT,          -- bronze | silver | gold
    total_files   INTEGER,
    files_with_issues INTEGER,
    pass_rate     NUMERIC(5,2),
    dimension_failures JSONB,
    report_json   JSONB
);

-- -----------------------------------------------------------------------------
-- Views for quick BI queries
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW vw_articles_by_source AS
    SELECT source, COUNT(*) AS article_count, MAX(collected_at) AS last_collected
    FROM articles_gold
    GROUP BY source
    ORDER BY article_count DESC;

CREATE OR REPLACE VIEW vw_articles_by_day AS
    SELECT DATE(published_at) AS day, COUNT(*) AS article_count
    FROM articles_gold
    WHERE published_at IS NOT NULL
    GROUP BY 1
    ORDER BY 1 DESC;

CREATE OR REPLACE VIEW vw_articles_by_language AS
    SELECT COALESCE(language, 'unknown') AS language, COUNT(*) AS article_count
    FROM articles_gold
    GROUP BY 1
    ORDER BY 2 DESC;

CREATE OR REPLACE VIEW vw_articles_by_category AS
    SELECT COALESCE(category, 'unknown') AS category, COUNT(*) AS article_count
    FROM articles_gold
    GROUP BY 1
    ORDER BY 2 DESC;
