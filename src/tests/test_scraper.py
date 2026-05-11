import json
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.scraper.scraper import parse_article, discover_article_links


def test_discover_bbc_links():
    html = Path('tests/sample_bbc.html').read_text(encoding='utf-8')
    links = discover_article_links(html, 'a.gs-c-promo-heading', 'https://www.bbc.com')
    assert len(links) >= 1
    assert links[0].startswith('https://') or links[0].startswith('http') or '/news' in links[0]


def test_parse_bbc_article():
    html = Path('tests/sample_bbc.html').read_text(encoding='utf-8')
    selectors = {'title_selector': 'h1', 'author_selector': '.byline__name', 'date_selector': 'time', 'content_selector': 'div.content'}
    article = parse_article(html, 'BBC News', selectors)
    assert article['title'] == 'Sample Article Title'
    assert 'sample' in article['content'].lower()


def test_parse_hespress_article():
    html = Path('tests/sample_hespress.html').read_text(encoding='utf-8')
    selectors = {'title_selector': 'h1.entry-title', 'author_selector': '.author', 'date_selector': 'time', 'content_selector': 'div.entry-content'}
    article = parse_article(html, 'Hespress', selectors)
    assert article['title'] == 'Hespress Title'
    assert article['author'] == 'By Fatima'
