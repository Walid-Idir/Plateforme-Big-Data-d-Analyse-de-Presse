"""
Data Governance — Data Catalog & Lineage Tracker

Responsibilities:
  - Auto-document datasets in each medallion layer (Bronze, Silver, Gold)
  - Track pipeline lineage (which Bronze file → which Silver file)
  - Generate a human-readable data catalog (JSON + Markdown)
  - Record data dictionaries for all known schemas
"""
import argparse
import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Data Dictionary (schema documentation)
# ---------------------------------------------------------------------------

DATA_DICTIONARY = {
    "bronze": {
        "description": "Raw scraped articles — no transformation applied.",
        "layer": "Bronze",
        "fields": {
            "title": {"type": "string", "nullable": True, "description": "Article headline"},
            "author": {"type": "string", "nullable": True, "description": "Author name(s)"},
            "date": {"type": "string", "nullable": True, "description": "Raw publication date string"},
            "content": {"type": "string", "nullable": True, "description": "Raw article body (may contain HTML)"},
            "source": {"type": "string", "nullable": False, "description": "Name of the news source"},
            "url": {"type": "string", "nullable": False, "description": "Canonical article URL"},
            "category": {"type": "string", "nullable": True, "description": "Article category / section"},
            "collected_at": {"type": "datetime", "nullable": False, "description": "UTC timestamp when article was scraped"},
        },
    },
    "silver": {
        "description": "Cleaned and enriched articles. HTML stripped, language detected, dates normalised.",
        "layer": "Silver",
        "fields": {
            "content_clean": {"type": "string", "nullable": True, "description": "HTML-stripped, whitespace-normalised content"},
            "language": {"type": "string", "nullable": True, "description": "ISO 639-1 language code (auto-detected)"},
            "word_count": {"type": "integer", "nullable": True, "description": "Number of words in content_clean"},
            "silver_processed_at": {"type": "datetime", "nullable": False, "description": "UTC timestamp of Silver processing"},
            "**inherits bronze fields**": {"type": "-", "nullable": True, "description": "All Bronze fields are preserved"},
        },
    },
    "gold": {
        "description": "Analytical tables aggregated from Silver for BI and dashboarding.",
        "layer": "Gold",
        "tables": {
            "articles_per_source": "Count of articles grouped by source",
            "articles_per_day": "Count of articles grouped by publication day",
            "articles_per_category": "Count of articles grouped by category",
            "articles_per_language": "Count of articles grouped by detected language",
            "top_keywords": "Top-N keyword frequencies across all articles",
            "daily_trends": "Articles per source per day — enables trend tracking",
        },
    },
}


# ---------------------------------------------------------------------------
# Dataset inventory
# ---------------------------------------------------------------------------

def _stat_dir(path: Path) -> dict:
    """Return basic statistics about a directory of JSON files."""
    files = list(path.glob("*.json"))
    total_bytes = sum(f.stat().st_size for f in files)
    return {
        "file_count": len(files),
        "total_size_bytes": total_bytes,
        "total_size_kb": round(total_bytes / 1024, 2),
        "sample_files": [f.name for f in files[:5]],
    }


def build_catalog(base_dir: Path) -> dict:
    """Scan bronze/silver/gold directories and build catalog."""
    layers = {}
    for layer in ("bronze", "silver", "gold"):
        layer_dir = base_dir / layer
        if layer_dir.exists():
            layers[layer] = {
                **DATA_DICTIONARY.get(layer, {}),
                "path": str(layer_dir),
                "stats": _stat_dir(layer_dir),
                "last_updated": datetime.utcnow().isoformat() + "Z",
            }
        else:
            layers[layer] = {
                **DATA_DICTIONARY.get(layer, {}),
                "path": str(layer_dir),
                "stats": {"file_count": 0},
                "note": "Directory not yet created",
            }

    catalog = {
        "catalog_id": str(uuid.uuid4()),
        "generated_at": datetime.now(timezone.utc).isoformat()[:19] + "Z",
        "project": "NewsDataPlatform",
        "description": "Auto-generated data catalog for the News Media Big Data Platform",
        "layers": layers,
        "pipeline_lineage": {
            "scraper": "Fetches articles from news sites → writes to Bronze",
            "silver_transform": "Reads Bronze → cleans, detects language → writes to Silver",
            "gold_aggregation": "Reads Silver → builds analytical tables → writes to Gold",
            "etl_load": "Reads Gold → loads into PostgreSQL Data Warehouse",
        },
        "data_sources": [
            {"name": "BBC News", "url": "https://www.bbc.com/news", "language": "en", "country": "UK"},
            {"name": "CNN", "url": "https://www.cnn.com/world", "language": "en", "country": "US"},
            {"name": "Reuters", "url": "https://www.reuters.com", "language": "en", "country": "International"},
            {"name": "Al Jazeera", "url": "https://www.aljazeera.com", "language": "en/ar", "country": "Qatar"},
            {"name": "Hespress", "url": "https://www.hespress.com", "language": "ar", "country": "MA"},
            {"name": "Akhbarona", "url": "https://akhbarona.com", "language": "ar", "country": "MA"},
            {"name": "Lakom", "url": "https://lakom.com", "language": "ar", "country": "MA"},
            {"name": "Barlamane", "url": "https://barlamane.com", "language": "ar", "country": "MA"},
        ],
        "governance_policies": {
            "retention": "Raw Bronze data retained for 90 days; Silver/Gold indefinitely",
            "pii": "Author names are retained; no personal user data collected",
            "access_control": "MinIO bucket policies restrict write access to scraper service account",
            "quality_gate": "Articles failing DQ checks are quarantined in data/quarantine/",
        },
    }
    return catalog


def save_catalog(catalog: dict, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(catalog, f, ensure_ascii=False, indent=2)


def catalog_to_markdown(catalog: dict) -> str:
    lines = [
        "# 📚 Data Catalog — News Media Platform",
        "",
        f"**Generated at:** {catalog['generated_at']}  ",
        f"**Project:** {catalog['project']}  ",
        "",
        f"> {catalog['description']}",
        "",
        "---",
        "",
        "## 🗂️ Medallion Layers",
        "",
    ]
    for layer_name, layer in catalog["layers"].items():
        emoji = {"bronze": "🥉", "silver": "🥈", "gold": "🥇"}.get(layer_name, "📦")
        lines.append(f"### {emoji} {layer_name.capitalize()} Layer")
        lines.append(f"- **Path:** `{layer.get('path', 'N/A')}`")
        lines.append(f"- **Description:** {layer.get('description', '')}")
        stats = layer.get("stats", {})
        lines.append(f"- **File count:** {stats.get('file_count', 0)}")
        lines.append(f"- **Total size:** {stats.get('total_size_kb', 0)} KB")
        lines.append("")

    lines += [
        "## 🔗 Pipeline Lineage",
        "",
    ]
    for step, desc in catalog["pipeline_lineage"].items():
        lines.append(f"- **{step}**: {desc}")
    lines.append("")

    lines += [
        "## 🌐 Data Sources",
        "",
        "| Source | URL | Language | Country |",
        "|--------|-----|----------|---------|",
    ]
    for src in catalog["data_sources"]:
        lines.append(f"| {src['name']} | {src['url']} | {src['language']} | {src['country']} |")
    lines.append("")

    lines += [
        "## 🔒 Governance Policies",
        "",
    ]
    for policy, value in catalog["governance_policies"].items():
        lines.append(f"- **{policy}**: {value}")
    lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Data Catalog & Governance")
    parser.add_argument("--data-dir", default="data", help="Base data directory (parent of bronze/silver/gold)")
    parser.add_argument("--output", default="data/catalog.json", help="Output catalog JSON path")
    parser.add_argument("--markdown", default="data/catalog.md", help="Output catalog Markdown path")
    args = parser.parse_args()

    catalog = build_catalog(Path(args.data_dir))
    save_catalog(catalog, Path(args.output))
    md = catalog_to_markdown(catalog)
    Path(args.markdown).write_text(md, encoding="utf-8")

    print(f"Catalog saved to {args.output}")
    print(f"Markdown saved to {args.markdown}")
    for layer, info in catalog["layers"].items():
        print(f"  [{layer}] {info['stats']['file_count']} files")


if __name__ == "__main__":
    main()
