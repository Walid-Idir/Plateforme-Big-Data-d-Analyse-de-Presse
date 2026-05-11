"""
Data Quality Reporter — covers three dimensions:

  1. Completeness  — required fields present and non-empty
  2. Consistency   — field values within expected ranges / formats
  3. Validity      — content length, URL format, date parseable

Produces a JSON report with per-article issues and summary statistics.
"""
import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List


# ---------------------------------------------------------------------------
# Individual check functions
# ---------------------------------------------------------------------------

def check_completeness(obj: dict) -> List[str]:
    """Return list of completeness issues."""
    issues = []
    if not obj.get("title", "").strip():
        issues.append("missing_title")
    if not (obj.get("content") or obj.get("content_clean", "")).strip():
        issues.append("missing_content")
    if not obj.get("date") and not obj.get("published_at") and not obj.get("collected_at"):
        issues.append("missing_date")
    if not obj.get("source", "").strip():
        issues.append("missing_source")
    if not obj.get("url", "").strip():
        issues.append("missing_url")
    return issues


def check_consistency(obj: dict) -> List[str]:
    """Return list of consistency issues."""
    issues = []
    # URL must start with http
    url = obj.get("url", "")
    if url and not url.startswith("http"):
        issues.append("invalid_url_scheme")

    # Date should be parseable
    for field in ("date", "published_at"):
        val = obj.get(field)
        if val:
            try:
                from dateutil import parser as dp
                dp.parse(val)
            except Exception:
                issues.append(f"unparseable_date_field_{field}")

    # Source should be non-numeric
    source = obj.get("source", "")
    if source and source.isdigit():
        issues.append("source_is_numeric")

    return issues


def check_validity(obj: dict) -> List[str]:
    """Return list of validity issues."""
    issues = []
    content = (obj.get("content_clean") or obj.get("content") or "").strip()

    if content and len(content) < 50:
        issues.append("content_too_short")

    if content and len(content) > 500_000:
        issues.append("content_suspiciously_long")

    title = (obj.get("title") or "").strip()
    if title and len(title) > 500:
        issues.append("title_too_long")

    # Check for HTML remnants in cleaned content
    if obj.get("content_clean") and re.search(r"<[a-zA-Z][^>]*>", obj["content_clean"]):
        issues.append("html_in_cleaned_content")

    return issues


# ---------------------------------------------------------------------------
# Dimension aggregation
# ---------------------------------------------------------------------------

DIMENSIONS = {
    "completeness": check_completeness,
    "consistency": check_consistency,
    "validity": check_validity,
}


def check_article(obj: dict) -> Dict[str, List[str]]:
    return {dim: fn(obj) for dim, fn in DIMENSIONS.items()}


# ---------------------------------------------------------------------------
# Directory runner
# ---------------------------------------------------------------------------

def run_dir(dir_path: Path) -> dict:
    articles_with_issues = {}
    dimension_counts = {dim: 0 for dim in DIMENSIONS}
    total = 0

    for f in sorted(dir_path.glob("*.json")):
        try:
            with open(f, "r", encoding="utf-8") as fh:
                obj = json.load(fh)
        except Exception as e:
            articles_with_issues[f.name] = {"parse_error": str(e)}
            continue

        total += 1
        result = check_article(obj)
        flat_issues = []
        for dim, issues in result.items():
            if issues:
                dimension_counts[dim] += 1
                flat_issues.extend(issues)
        if flat_issues:
            articles_with_issues[f.name] = {
                "dimensions": result,
                "all_issues": flat_issues,
            }

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat()[:19] + "Z",
        "directory": str(dir_path),
        "total_articles": total,
        "articles_with_issues": len(articles_with_issues),
        "pass_rate": round((total - len(articles_with_issues)) / total * 100, 2) if total else 0,
        "dimension_failure_counts": dimension_counts,
        "issues": articles_with_issues,
    }
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Data Quality Reporter")
    parser.add_argument("--input", required=True, help="Directory of JSON articles")
    parser.add_argument("--output", default=None, help="Output JSON report path")
    args = parser.parse_args()

    report = run_dir(Path(args.input))
    print(f"\n=== DQ Report ===")
    print(f"Total articles  : {report['total_articles']}")
    print(f"With issues     : {report['articles_with_issues']}")
    print(f"Pass rate       : {report['pass_rate']}%")
    print(f"Dimension failures: {report['dimension_failure_counts']}")

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        with open(out, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        print(f"\nFull report written to {out}")
    else:
        print("\nPer-article issues:")
        for name, issues in report["issues"].items():
            print(f"  {name}: {issues.get('all_issues', issues)}")


if __name__ == "__main__":
    main()
