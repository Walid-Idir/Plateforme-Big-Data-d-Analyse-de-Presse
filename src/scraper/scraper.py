"""Simple, configurable scraper that writes raw articles to data/bronze and optionally uploads to MinIO."""
import argparse
import json
import os
import time
import uuid
from datetime import datetime
from pathlib import Path

import requests
import yaml
from bs4 import BeautifulSoup

try:
    from minio import Minio
except Exception:
    Minio = None


def load_config(path):
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def fetch(url, timeout=15):
    resp = requests.get(url, timeout=timeout, headers={'User-Agent': 'Mozilla/5.0'})
    resp.raise_for_status()
    return resp.text


def parse_article(html, base_url, selectors):
    soup = BeautifulSoup(html, 'html.parser')
    title = (soup.select_one(selectors.get('title_selector')) or soup.title)
    title_text = title.get_text(strip=True) if title else ''
    author_el = soup.select_one(selectors.get('author_selector'))
    date_el = soup.select_one(selectors.get('date_selector'))
    content_el = soup.select_one(selectors.get('content_selector'))
    content = content_el.get_text(separator='\n', strip=True) if content_el else ''
    return {
        'title': title_text,
        'author': author_el.get_text(strip=True) if author_el else None,
        'date': date_el.get('datetime') if date_el and date_el.has_attr('datetime') else (date_el.get_text(strip=True) if date_el else None),
        'content': content,
        'source': base_url,
    }


def discover_article_links(list_html, selector, base_url):
    soup = BeautifulSoup(list_html, 'html.parser')
    links = []
    for a in soup.select(selector):
        href = a.get('href')
        if not href:
            continue
        if href.startswith('http'):
            links.append(href)
        else:
            links.append(requests.compat.urljoin(base_url, href))
    return list(dict.fromkeys(links))


def save_raw(article, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    fname = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}.json"
    path = out_dir / fname
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(article, f, ensure_ascii=False, indent=2)
    return path


def upload_minio(minio_cfg, bucket, path: Path):
    if Minio is None:
        print('minio library not available; skipping upload')
        return
    client = Minio(minio_cfg['endpoint'], access_key=minio_cfg['access_key'], secret_key=minio_cfg['secret_key'], secure=str(minio_cfg.get('secure', False)).lower() in ('1','true','yes'))
    try:
        client.make_bucket(bucket)
    except Exception:
        pass
    object_name = f"bronze/{path.name}"
    client.fput_object(bucket, object_name, str(path))


def run(config_path, out_base='data/bronze'):
    cfg = load_config(config_path)
    sites = cfg.get('sites', [])
    minio_cfg = cfg.get('minio') or {}
    out_base = Path(out_base)
    for site in sites:
        print('Scraping', site.get('name'))
        try:
            list_html = fetch(site['list_url'])
            links = discover_article_links(list_html, site.get('article_selector'), site['list_url'])
            print(f'Found {len(links)} links (first 5):', links[:5])
            for url in links[:20]:
                try:
                    html = fetch(url)
                    article = parse_article(html, site.get('name', site['list_url']), site)
                    article['url'] = url
                    article['collected_at'] = datetime.utcnow().isoformat() + 'Z'
                    path = save_raw(article, out_base)
                    print('Saved raw:', path)
                    if minio_cfg and os.environ.get('MINIO_ENDPOINT'):
                        upload_minio(minio_cfg, 'news-data', path)
                except Exception as e:
                    print('Article error', url, e)
        except Exception as e:
            print('Site error', site.get('name'), e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--out', default='data/bronze')
    args = parser.parse_args()
    run(args.config, args.out)
