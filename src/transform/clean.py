"""Transformations: bronze -> silver (cleaning) and simple gold aggregations."""
import argparse
import json
import re
from pathlib import Path
from collections import Counter

from bs4 import BeautifulSoup
from langdetect import detect


def strip_html(text):
    return BeautifulSoup(text or '', 'html.parser').get_text(separator=' ', strip=True)


def normalize_text(text):
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def process_file(path: Path, out_dir: Path):
    with open(path, 'r', encoding='utf-8') as f:
        obj = json.load(f)
    content = obj.get('content') or ''
    content_clean = strip_html(content)
    content_clean = normalize_text(content_clean)
    lang = None
    try:
        lang = detect(content_clean) if content_clean else None
    except Exception:
        lang = None
    obj['content_clean'] = content_clean
    obj['language'] = lang
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / path.name
    with open(out_path, 'w', encoding='utf-8') as o:
        json.dump(obj, o, ensure_ascii=False, indent=2)
    return out_path


def aggregate_keywords(silver_dir: Path, out_dir: Path, top_n=50):
    counter = Counter()
    for f in silver_dir.glob('*.json'):
        with open(f, 'r', encoding='utf-8') as fh:
            obj = json.load(fh)
            words = (obj.get('content_clean') or '').lower().split()
            counter.update(w.strip('.,;:!?()[]"\'"') for w in words if len(w) > 3)
    top = counter.most_common(top_n)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / 'top_keywords.json'
    with open(out_path, 'w', encoding='utf-8') as o:
        json.dump(top, o, ensure_ascii=False, indent=2)
    return out_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--aggregate', action='store_true')
    args = parser.parse_args()
    inp = Path(args.input)
    out = Path(args.output)
    if args.aggregate:
        p = aggregate_keywords(inp, out)
        print('Wrote', p)
    else:
        out.mkdir(parents=True, exist_ok=True)
        for f in inp.glob('*.json'):
            p = process_file(f, out)
            print('Processed', p)


if __name__ == '__main__':
    main()
