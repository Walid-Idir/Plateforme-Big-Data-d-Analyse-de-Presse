"""NLP enrichment helpers: keywords, entities, embeddings (best-effort)."""
from typing import List
import re

try:
    import spacy
    nlp_spacy = spacy.load('en_core_web_sm')
except Exception:
    nlp_spacy = None

try:
    from sklearn.feature_extraction.text import TfidfVectorizer
except Exception:
    TfidfVectorizer = None

def _tokenize(text: str):
    text = (text or '').lower()
    tokens = re.findall(r"\b[a-zA-Z]{4,}\b", text)
    return tokens

def extract_keywords(text: str, top_n=10) -> List[str]:
    text = text or ''
    if nlp_spacy:
        doc = nlp_spacy(text)
        # simple noun chunk based keywords
        chunks = [chunk.text.strip() for chunk in doc.noun_chunks]
        return chunks[:top_n]
    if TfidfVectorizer:
        try:
            vec = TfidfVectorizer(max_features=1000, stop_words='english')
            X = vec.fit_transform([text])
            scores = list(zip(vec.get_feature_names_out(), X.toarray().sum(axis=0)))
            scores.sort(key=lambda x: x[1], reverse=True)
            return [w for w, s in scores[:top_n]]
        except Exception:
            pass
    # fallback: frequency
    tokens = _tokenize(text)
    freq = {}
    for t in tokens:
        freq[t] = freq.get(t, 0) + 1
    return sorted(freq, key=freq.get, reverse=True)[:top_n]

def extract_entities(text: str):
    if nlp_spacy:
        doc = nlp_spacy(text)
        return [{'text': ent.text, 'label': ent.label_} for ent in doc.ents]
    return []

def embed_texts(texts):
    try:
        from sentence_transformers import SentenceTransformer
        model = SentenceTransformer('all-MiniLM-L6-v2')
        return model.encode(texts)
    except Exception:
        return None
