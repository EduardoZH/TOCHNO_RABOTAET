import os
import pytest

# Ensure shared modules are importable
os.environ.setdefault("RABBIT_HOST", "localhost")
os.environ.setdefault("REDIS_URL", "redis://localhost:6379")
os.environ.setdefault("QDRANT_HOST", "localhost")

DATASET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)),
    "Мобайл_X-Prod.xlsx.csv",
)


@pytest.fixture
def sample_post():
    return {
        "post_id": "test-post-001",
        "projectId": "project-abc",
        "keywords": ["мошен", "недвижимост", "афер"],
        "exclusions": ["спам", "реклама"],
        "title": "Мошенники продают несуществующие квартиры",
        "content": "Аферисты обманули десятки покупателей недвижимости в Москве.",
        "type": "article",
        "url": "https://example.com/post/1",
        "timestamp": 1710500000.0,
    }


@pytest.fixture
def sample_post_irrelevant():
    return {
        "post_id": "test-post-002",
        "projectId": "project-abc",
        "keywords": ["мошен", "недвижимост"],
        "exclusions": [],
        "title": "Котики захватили интернет",
        "content": "Смешные видео с кошками набирают миллионы просмотров.",
        "type": "post",
        "url": "https://example.com/cats",
        "timestamp": 1710500000.0,
    }


@pytest.fixture(scope="session")
def dataset_sample():
    """Load a sample of 200 rows from the real dataset (cached per session)."""
    try:
        import pandas as pd
        if os.path.exists(DATASET_PATH):
            df = pd.read_csv(DATASET_PATH, nrows=5000, encoding="utf-8")
            return df.sample(n=min(200, len(df)), random_state=42)
    except Exception:
        pass
    return None
