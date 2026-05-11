"""
Tests unitaires — News Media Big Data Platform
"""
import json
import tempfile
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# scraper tests
# ---------------------------------------------------------------------------

from src.scraper.scraper import parse_article, discover_article_links


def _bbc_html():
    return Path("tests/sample_bbc.html").read_text(encoding="utf-8")


def _hespress_html():
    return Path("tests/sample_hespress.html").read_text(encoding="utf-8")


BBC_SELECTORS = {
    "title_selector": "h1",
    "author_selector": ".byline__name",
    "date_selector": "time",
    "content_selector": "article",
}


def test_parse_bbc_title():
    article = parse_article(_bbc_html(), "BBC News", BBC_SELECTORS)
    assert article["title"] == "Sample Article Title"


def test_parse_bbc_date():
    article = parse_article(_bbc_html(), "BBC News", BBC_SELECTORS)
    assert article["date"] == "2026-05-04T10:00:00Z"


def test_parse_bbc_content_non_empty():
    article = parse_article(_bbc_html(), "BBC News", BBC_SELECTORS)
    assert article["content"]
    assert "sample" in article["content"].lower()


def test_discover_links_bbc():
    links = discover_article_links(_bbc_html(), "a.gs-c-promo-heading", "https://www.bbc.com")
    assert len(links) >= 1
    assert any("sample-article" in l for l in links)


# ---------------------------------------------------------------------------
# medallion / transform tests
# ---------------------------------------------------------------------------

from src.transform.medallion import bronze_to_silver, strip_html, normalize_whitespace


def test_strip_html_removes_tags():
    assert strip_html("<p>Hello <b>world</b></p>") == "Hello world"


def test_strip_html_handles_empty():
    assert strip_html("") == ""
    assert strip_html(None) == ""


def test_normalize_whitespace():
    assert normalize_whitespace("  foo   bar  ") == "foo bar"


def test_bronze_to_silver_adds_fields():
    bronze = {
        "title": "Test Article",
        "content": "<p>This is a <b>test</b> article with enough content to be valid.</p>",
        "source": "TestSite",
        "url": "https://test.com/article",
        "collected_at": "2026-05-05T10:00:00Z",
    }
    silver = bronze_to_silver(bronze)
    assert "content_clean" in silver
    assert "<" not in silver["content_clean"], "HTML should be stripped"
    assert "word_count" in silver
    assert silver["word_count"] > 0
    assert "silver_processed_at" in silver


def test_bronze_to_silver_language_detected():
    bronze = {
        "content": "This is a long English text about news and current events in the world.",
        "source": "Test",
        "url": "https://test.com",
        "collected_at": "2026-05-05T10:00:00Z",
    }
    silver = bronze_to_silver(bronze)
    # Language detection may return None if langdetect unavailable — just check key exists
    assert "language" in silver


def test_medallion_process_dir(tmp_path):
    from src.transform.medallion import process_bronze_dir, build_gold

    # Write 3 bronze articles
    for i in range(3):
        art = {
            "title": f"Article {i}",
            "content": f"<p>This is the content of article number {i}. It should be long enough to pass DQ.</p>",
            "source": "TestSite",
            "url": f"https://test.com/article-{i}",
            "category": "World",
            "collected_at": "2026-05-05T10:00:00Z",
        }
        (tmp_path / f"article_{i}.json").write_text(json.dumps(art), encoding="utf-8")

    silver_dir = tmp_path / "silver"
    processed = process_bronze_dir(tmp_path, silver_dir)
    assert len(processed) == 3

    gold_dir = tmp_path / "gold"
    tables = build_gold(silver_dir, gold_dir)
    assert "articles_per_source" in tables
    assert "top_keywords" in tables
    assert tables["articles_per_source"][0]["source"] == "TestSite"
    assert tables["articles_per_source"][0]["count"] == 3


# ---------------------------------------------------------------------------
# data quality tests
# ---------------------------------------------------------------------------

from src.quality.dq_report import check_completeness, check_consistency, check_validity, run_dir


def _valid_article():
    return {
        "title": "Valid Article Title",
        "content_clean": "This is a sufficiently long content for the article to pass all quality checks without any issue.",
        "source": "BBC News",
        "url": "https://www.bbc.com/news/test-article",
        "date": "2026-05-04T10:00:00Z",
        "collected_at": "2026-05-05T10:00:00Z",
    }


def test_dq_valid_article_no_issues():
    art = _valid_article()
    assert check_completeness(art) == []
    assert check_consistency(art) == []
    assert check_validity(art) == []


def test_dq_missing_title():
    art = _valid_article()
    art["title"] = ""
    issues = check_completeness(art)
    assert "missing_title" in issues


def test_dq_missing_date():
    art = _valid_article()
    del art["date"]
    del art["collected_at"]
    issues = check_completeness(art)
    assert "missing_date" in issues


def test_dq_content_too_short():
    art = _valid_article()
    art["content_clean"] = "short"
    issues = check_validity(art)
    assert "content_too_short" in issues


def test_dq_invalid_url_scheme():
    art = _valid_article()
    art["url"] = "ftp://bad-url.com/article"
    issues = check_consistency(art)
    assert "invalid_url_scheme" in issues


def test_dq_run_dir(tmp_path):
    # Write one valid and one invalid article
    (tmp_path / "valid.json").write_text(json.dumps(_valid_article()), encoding="utf-8")
    (tmp_path / "invalid.json").write_text(json.dumps({"source": "Test"}), encoding="utf-8")

    report = run_dir(tmp_path)
    assert report["total_articles"] == 2
    assert report["articles_with_issues"] >= 1
    assert "invalid.json" in report["issues"]


# ---------------------------------------------------------------------------
# governance tests
# ---------------------------------------------------------------------------

from src.governance.catalog import build_catalog, catalog_to_markdown


def test_catalog_structure(tmp_path):
    catalog = build_catalog(tmp_path)
    assert "layers" in catalog
    assert "bronze" in catalog["layers"]
    assert "silver" in catalog["layers"]
    assert "gold" in catalog["layers"]
    assert "pipeline_lineage" in catalog
    assert "data_sources" in catalog


def test_catalog_markdown(tmp_path):
    catalog = build_catalog(tmp_path)
    md = catalog_to_markdown(catalog)
    assert "Bronze" in md
    assert "Silver" in md
    assert "Gold" in md
    assert "BBC News" in md
