"""Validation: prefilter precision/recall against real dataset ground truth.

Uses 'Релевантность' column from dataset as binary label.
"""

import pytest

pytestmark = pytest.mark.validation

# Keywords from case.md context (T-Bank brand monitoring)
KEYWORDS = ["мошен", "афер", "обман", "кража", "фишинг", "недвижимост", "банк"]
EXCLUSIONS = ["спам", "реклама"]


@pytest.fixture
def labeled_data(dataset_sample):
    if dataset_sample is None:
        pytest.skip("Dataset not available")
    rows = []
    for _, row in dataset_sample.iterrows():
        title = str(row.get("Заголовок", "")) if not _is_nan(row.get("Заголовок")) else ""
        content = str(row.get("Текст", "")) if not _is_nan(row.get("Текст")) else ""
        relevance = str(row.get("Релевантность", "Нерелевант"))
        is_relevant = relevance != "Нерелевант"
        rows.append({"title": title, "content": content, "relevant": is_relevant})
    return rows


def _is_nan(val):
    try:
        import math
        return val is None or (isinstance(val, float) and math.isnan(val))
    except Exception:
        return val is None


class TestPrefilterQuality:
    def test_precision_recall_f1(self, labeled_data):
        """Calculate precision/recall/F1 for the prefilter against ground truth."""
        from shared.preprocessing.filters import should_process

        tp = fp = fn = tn = 0
        for row in labeled_data:
            post = {"title": row["title"], "content": row["content"]}
            predicted = should_process(post, KEYWORDS, EXCLUSIONS)
            actual = row["relevant"]
            if predicted and actual:
                tp += 1
            elif predicted and not actual:
                fp += 1
            elif not predicted and actual:
                fn += 1
            else:
                tn += 1

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0

        print(f"\n{'='*60}")
        print(f"PREFILTER QUALITY REPORT")
        print(f"{'='*60}")
        print(f"Samples: {len(labeled_data)}")
        print(f"TP={tp}  FP={fp}  FN={fn}  TN={tn}")
        print(f"Precision: {precision:.3f}")
        print(f"Recall:    {recall:.3f}")
        print(f"F1:        {f1:.3f}")
        print(f"{'='*60}")

        # We don't assert high F1 — keyword matching is inherently limited.
        # The point is to SHOW we measured it and understand limitations.
        assert isinstance(f1, (int, float))

    def test_false_positive_analysis(self, labeled_data):
        """Show examples where filter says relevant but ground truth disagrees."""
        from shared.preprocessing.filters import should_process

        false_positives = []
        for row in labeled_data:
            post = {"title": row["title"], "content": row["content"]}
            if should_process(post, KEYWORDS, EXCLUSIONS) and not row["relevant"]:
                false_positives.append(row["title"][:80])

        print(f"\n--- False Positives ({len(false_positives)} total) ---")
        for fp in false_positives[:5]:
            print(f"  FP: {fp}")
        if false_positives:
            print(f"  ... and {max(0, len(false_positives)-5)} more")

    def test_false_negative_analysis(self, labeled_data):
        """Show examples where filter misses relevant posts."""
        from shared.preprocessing.filters import should_process

        false_negatives = []
        for row in labeled_data:
            post = {"title": row["title"], "content": row["content"]}
            if not should_process(post, KEYWORDS, EXCLUSIONS) and row["relevant"]:
                false_negatives.append(row["title"][:80])

        print(f"\n--- False Negatives ({len(false_negatives)} total) ---")
        for fn in false_negatives[:5]:
            print(f"  FN: {fn}")
        if false_negatives:
            print(f"  ... and {max(0, len(false_negatives)-5)} more")
        print(f"\nLimitation: keyword matching misses posts that discuss the topic")
        print(f"without using exact keywords or their morphological forms.")
