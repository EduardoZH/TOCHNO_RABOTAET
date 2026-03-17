"""Model Quality Validation — generates a quality report from real dataset.

Usage: PYTHONPATH=. python scripts/validate_model_quality.py
"""

import os
import sys
import math
import time
from collections import defaultdict

DATASET_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            "Мобайл_X-Prod.xlsx.csv")
KEYWORDS = ["мошен", "афер", "обман", "кража", "фишинг", "банк"]
EXCLUSIONS = ["спам", "реклама"]
SAMPLE_SIZE = 5000


def _is_nan(val):
    try:
        return val is None or (isinstance(val, float) and math.isnan(val))
    except Exception:
        return val is None


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def validate_prefilter(df):
    from shared.preprocessing.filters import should_process

    print("\n## 1. Prefilter Quality (keyword matching)")
    print()

    tp = fp = fn = tn = 0
    false_positives = []
    false_negatives = []

    for _, row in df.iterrows():
        title = str(row.get("Заголовок", "")) if not _is_nan(row.get("Заголовок")) else ""
        content = str(row.get("Текст", "")) if not _is_nan(row.get("Текст")) else ""
        relevance = str(row.get("Релевантность", "Нерелевант"))
        is_relevant = relevance != "Нерелевант"

        post = {"title": title, "content": content}
        predicted = should_process(post, KEYWORDS, EXCLUSIONS)

        if predicted and is_relevant:
            tp += 1
        elif predicted and not is_relevant:
            fp += 1
            if len(false_positives) < 5:
                false_positives.append(title[:80] or content[:80])
        elif not predicted and is_relevant:
            fn += 1
            if len(false_negatives) < 5:
                false_negatives.append(title[:80] or content[:80])
        else:
            tn += 1

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

    print(f"| Metric | Value |")
    print(f"|--------|-------|")
    print(f"| Samples | {len(df)} |")
    print(f"| TP | {tp} |")
    print(f"| FP | {fp} |")
    print(f"| FN | {fn} |")
    print(f"| TN | {tn} |")
    print(f"| Precision | {precision:.3f} |")
    print(f"| Recall | {recall:.3f} |")
    print(f"| F1 | {f1:.3f} |")

    if false_positives:
        print(f"\n**Top False Positives** (filter says relevant, actually not):")
        for ex in false_positives:
            print(f"- {ex}")

    if false_negatives:
        print(f"\n**Top False Negatives** (filter misses relevant post):")
        for ex in false_negatives:
            print(f"- {ex}")

    print(f"\n**Known Limitations:**")
    print(f"- Keyword matching cannot capture semantic meaning")
    print(f"- Partial stems may over-match (e.g., 'банк' matches 'банка' (jar))")
    print(f"- Non-standard spelling or transliteration not handled")


def validate_dedup(df):
    from shared.hashing.simhash import compute_simhash, hamming_distance

    print("\n\n## 2. Deduplication Quality (SimHash)")
    print()

    texts = []
    for _, row in df.iterrows():
        t = str(row.get("Текст", ""))
        if len(t) > 50:
            texts.append(t)
        if len(texts) >= 200:
            break

    # Check exact duplicate detection
    exact_ok = 0
    for text in texts[:20]:
        h1 = compute_simhash(text)
        h2 = compute_simhash(text)
        if h1 == h2:
            exact_ok += 1

    print(f"| Metric | Value |")
    print(f"|--------|-------|")
    print(f"| Exact duplicate detection | {exact_ok}/20 |")

    # Hamming distance distribution
    distances = []
    for i in range(min(50, len(texts))):
        for j in range(i + 1, min(50, len(texts))):
            h1 = compute_simhash(texts[i])
            h2 = compute_simhash(texts[j])
            distances.append(hamming_distance(h1, h2))

    if distances:
        avg_dist = sum(distances) / len(distances)
        below_3 = sum(1 for d in distances if d <= 3)
        print(f"| Avg Hamming distance (random pairs) | {avg_dist:.1f} |")
        print(f"| Pairs with distance ≤ 3 (potential dups) | {below_3}/{len(distances)} |")

    print(f"\n**Known Limitations:**")
    print(f"- Threshold=3 may produce false positives on short texts")
    print(f"- Completely rewritten duplicates (same meaning, different words) not detected")


def validate_embeddings(df):
    print("\n\n## 3. Embedding Quality (RuBERT-tiny2)")
    print()

    try:
        from shared.embeddings.embedder import text_to_embedding
    except Exception as e:
        print(f"Model loading failed: {e}")
        print("Skipping embedding validation.")
        return

    kw_emb = text_to_embedding(" ".join(KEYWORDS))
    relevant_scores = []
    irrelevant_scores = []

    count = 0
    for _, row in df.iterrows():
        text = str(row.get("Текст", ""))
        if len(text) < 20:
            continue
        relevance = str(row.get("Релевантность", "Нерелевант"))
        emb = text_to_embedding(text[:500])
        score = _cosine(emb, kw_emb) * 100

        if relevance != "Нерелевант":
            relevant_scores.append(score)
        else:
            irrelevant_scores.append(score)
        count += 1
        if count >= 100:
            break

    print(f"| Metric | Value |")
    print(f"|--------|-------|")
    print(f"| Samples | {count} |")
    if relevant_scores:
        print(f"| Mean relevancy (relevant posts) | {sum(relevant_scores)/len(relevant_scores):.1f} |")
    if irrelevant_scores:
        print(f"| Mean relevancy (irrelevant posts) | {sum(irrelevant_scores)/len(irrelevant_scores):.1f} |")
    if relevant_scores and irrelevant_scores:
        sep = sum(relevant_scores)/len(relevant_scores) - sum(irrelevant_scores)/len(irrelevant_scores)
        print(f"| Separation | {sep:.1f} |")

    print(f"\n**Known Limitations:**")
    print(f"- RuBERT-tiny2 has 312-dim vectors (compact but limited expressiveness)")
    print(f"- Max 512 tokens — long texts are truncated")
    print(f"- Keyword embedding is a joint embedding of all keywords (may dilute)")


def main():
    import pandas as pd

    if not os.path.exists(DATASET_PATH):
        print(f"Dataset not found: {DATASET_PATH}")
        return 1

    print("# Model Quality Report")
    print(f"\nDataset: {os.path.basename(DATASET_PATH)}")
    print(f"Sample size: {SAMPLE_SIZE}")
    print(f"Keywords: {KEYWORDS}")

    t0 = time.time()
    df = pd.read_csv(DATASET_PATH, nrows=SAMPLE_SIZE, encoding="utf-8")
    print(f"Loaded {len(df)} rows in {time.time()-t0:.1f}s")

    validate_prefilter(df)
    validate_dedup(df)
    validate_embeddings(df)

    print(f"\n\n## 4. Overall Model Boundaries")
    print()
    print("| Boundary | Behavior |")
    print("|----------|----------|")
    print("| Empty text | Prefilter rejects, SimHash=0, embedding=zero vector |")
    print("| Non-Russian text | Prefilter rejects (no keyword match), embedding works but low relevancy |")
    print("| Emoji-only text | Prefilter rejects, SimHash=0, embedding produces valid vector |")
    print("| Very long text (>10K chars) | Processed, but RuBERT truncates at 512 tokens |")
    print("| SQL injection / HTML | Treated as text, no security risk (no DB queries) |")
    print("| Missing fields | Graceful handling via .get() defaults |")
    print("| Model unavailable | Hash-based fallback embedding (degraded quality) |")
    print("| Redis down | Dedup passes through (no dedup, no data loss) |")
    print("| Qdrant down | Clustering assigns 'unknown' cluster |")

    print(f"\n---\nReport generated in {time.time()-t0:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
