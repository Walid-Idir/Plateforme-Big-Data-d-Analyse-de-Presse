"""
Medallion Architecture Pipeline: Bronze → Silver → Gold

Bronze : raw scraped articles (JSON)
Silver : cleaned, normalised, language-detected articles
Gold   : analytical tables (trends, top keywords, articles per source/day/category)
"""
import argparse
import json
import re
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

try:
    from langdetect import detect as _detect_lang
except ImportError:
    _detect_lang = None

# ---------------------------------------------------------------------------
# Bronze helpers
# ---------------------------------------------------------------------------

def save_bronze(article: dict, out_dir: Path) -> Path:
    """Persist a raw article dict to the Bronze layer."""
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.json"
    path = out_dir / fname
    with open(path, "w", encoding="utf-8") as f:
        json.dump(article, f, ensure_ascii=False, indent=2)
    return path


# ---------------------------------------------------------------------------
# Silver transformations
# ---------------------------------------------------------------------------

def strip_html(text: str) -> str:
    """Remove HTML tags from text."""
    return BeautifulSoup(text or "", "html.parser").get_text(separator=" ", strip=True)


def normalize_whitespace(text: str) -> str:
    text = re.sub(r"\s+", " ", text or "")
    return text.strip()


def detect_language(text: str) -> Optional[str]:
    if not text or len(text) < 20:
        return None
    if _detect_lang:
        try:
            return _detect_lang(text)
        except Exception:
            return None
    return None


def normalize_date(raw_date: str) -> Optional[str]:
    """Try to parse a date string and return ISO8601 or None."""
    if not raw_date:
        return None
    from dateutil import parser as dparser
    try:
        return dparser.parse(raw_date).isoformat()
    except Exception:
        return None


def bronze_to_silver(bronze_obj: dict) -> dict:
    """Apply all Silver-layer transformations to a Bronze article dict."""
    obj = dict(bronze_obj)  # copy

    # HTML stripping & normalisation
    raw_content = obj.get("content") or ""
    content_clean = normalize_whitespace(strip_html(raw_content))
    obj["content_clean"] = content_clean

    # Language detection
    obj["language"] = detect_language(content_clean)

    # Date normalisation
    for field in ("date", "published_at"):
        if obj.get(field):
            obj[field] = normalize_date(obj[field])

    # Word count
    obj["word_count"] = len(content_clean.split()) if content_clean else 0

    obj["silver_processed_at"] = datetime.now(timezone.utc).isoformat()[:19] + "Z"
    return obj


def process_bronze_dir(bronze_dir: Path, silver_dir: Path) -> list:
    """Convert all Bronze JSON files to Silver."""
    silver_dir.mkdir(parents=True, exist_ok=True)
    processed = []
    for f in sorted(bronze_dir.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
            silver_obj = bronze_to_silver(obj)
            out = silver_dir / f.name
            with open(out, "w", encoding="utf-8") as fh:
                json.dump(silver_obj, fh, ensure_ascii=False, indent=2)
            processed.append(out)
        except Exception as e:
            print(f"[Silver] Error processing {f.name}: {e}")
    return processed


# ---------------------------------------------------------------------------
# Gold aggregations
# ---------------------------------------------------------------------------

def _load_silver(silver_dir: Path) -> list:
    articles = []
    for f in sorted(silver_dir.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                articles.append(json.load(fh))
        except Exception:
            pass
    return articles


def _extract_day(article: dict) -> Optional[str]:
    for field in ("date", "published_at", "collected_at"):
        val = article.get(field)
        if val:
            try:
                return val[:10]  # YYYY-MM-DD
            except Exception:
                pass
    return None


def build_articles_per_source(articles: list) -> list:
    counter = Counter(a.get("source", "unknown") for a in articles)
    return [{"source": k, "count": v} for k, v in counter.most_common()]


def build_articles_per_day(articles: list) -> list:
    counter = Counter()
    for a in articles:
        day = _extract_day(a)
        if day:
            counter[day] += 1
    return [{"day": k, "count": v} for k, v in sorted(counter.items())]


def build_articles_per_category(articles: list) -> list:
    counter = Counter(a.get("category", "unknown") for a in articles)
    return [{"category": k, "count": v} for k, v in counter.most_common()]


def build_articles_per_language(articles: list) -> list:
    counter = Counter(a.get("language", "unknown") for a in articles)
    return [{"language": k, "count": v} for k, v in counter.most_common()]


def build_top_keywords(articles: list, top_n: int = 100) -> list:
    STOPWORDS = {
        "that", "this", "with", "from", "have", "been", "will", "were", "they",
        "their", "there", "than", "then", "when", "what", "where", "which",
        "also", "some", "more", "other", "into", "over", "after", "said",
        "about", "just", "would", "could", "should", "على", "في", "من", "إلى",
    }
    counter = Counter()
    for a in articles:
        text = (a.get("content_clean") or a.get("content") or "").lower()
        words = re.findall(r"\b[a-zA-Zأ-ي]{4,}\b", text)
        counter.update(w for w in words if w not in STOPWORDS)
    return [{"keyword": w, "count": c} for w, c in counter.most_common(top_n)]


def build_daily_trends(articles: list) -> list:
    """Articles per source per day — useful for trend analysis."""
    data = defaultdict(lambda: defaultdict(int))
    for a in articles:
        day = _extract_day(a)
        src = a.get("source", "unknown")
        if day:
            data[day][src] += 1
    result = []
    for day in sorted(data):
        for src, cnt in data[day].items():
            result.append({"day": day, "source": src, "count": cnt})
    return result


def build_gold(silver_dir: Path, gold_dir: Path) -> dict:
    """Produce all Gold analytical tables from Silver data."""
    gold_dir.mkdir(parents=True, exist_ok=True)
    articles = _load_silver(silver_dir)
    if not articles:
        print("[Gold] No Silver articles found.")
        return {}

    tables = {
        "articles_per_source": build_articles_per_source(articles),
        "articles_per_day": build_articles_per_day(articles),
        "articles_per_category": build_articles_per_category(articles),
        "articles_per_language": build_articles_per_language(articles),
        "top_keywords": build_top_keywords(articles),
        "daily_trends": build_daily_trends(articles),
    }

    for name, data in tables.items():
        out = gold_dir / f"{name}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[Gold] Wrote {name}.json ({len(data)} records)")

    # Also copy individual silver articles to gold articles dir
    gold_articles = gold_dir / "articles"
    gold_articles.mkdir(exist_ok=True)
    for a in articles:
        fname = f"{a.get('url', uuid.uuid4().hex)}"
        fname = re.sub(r"[^a-zA-Z0-9_\-]", "_", fname)[-60:]
        out = gold_articles / f"{fname}.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(a, f, ensure_ascii=False, indent=2)

    return tables


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Medallion pipeline: bronze→silver→gold")
    sub = parser.add_subparsers(dest="command", required=True)

    s2s = sub.add_parser("silver", help="Process bronze→silver")
    s2s.add_argument("--bronze", required=True)
    s2s.add_argument("--silver", required=True)

    g2g = sub.add_parser("gold", help="Build gold analytical tables from silver")
    g2g.add_argument("--silver", required=True)
    g2g.add_argument("--gold", required=True)

    full = sub.add_parser("full", help="Run full pipeline: bronze→silver→gold")
    full.add_argument("--bronze", required=True)
    full.add_argument("--silver", required=True)
    full.add_argument("--gold", required=True)

    args = parser.parse_args()

    if args.command == "silver":
        processed = process_bronze_dir(Path(args.bronze), Path(args.silver))
        print(f"[Silver] Processed {len(processed)} files.")

    elif args.command == "gold":
        tables = build_gold(Path(args.silver), Path(args.gold))
        print(f"[Gold] Built {len(tables)} tables.")

    elif args.command == "full":
        processed = process_bronze_dir(Path(args.bronze), Path(args.silver))
        print(f"[Silver] Processed {len(processed)} files.")
        tables = build_gold(Path(args.silver), Path(args.gold))
        print(f"[Gold] Built {len(tables)} tables.")


if __name__ == "__main__":
    main()
