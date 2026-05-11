"""Data quality checks for scraped articles."""
import json
from pathlib import Path


def check_completeness(json_path: Path):
    with open(json_path, 'r', encoding='utf-8') as f:
        obj = json.load(f)
    issues = []
    if not obj.get('title'):
        issues.append('missing_title')
    if not obj.get('content') and not obj.get('content_clean'):
        issues.append('missing_content')
    if not obj.get('date') and not obj.get('collected_at'):
        issues.append('missing_date')
    if obj.get('content') and len(obj.get('content')) < 50:
        issues.append('content_too_short')
    return issues


def run_dir(dir_path: Path):
    report = {}
    for f in dir_path.glob('*.json'):
        issues = check_completeness(f)
        if issues:
            report[str(f.name)] = issues
    return report


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    args = parser.parse_args()
    rep = run_dir(Path(args.input))
    print('DQ issues found:', len(rep))
    for k, v in rep.items():
        print(k, v)
