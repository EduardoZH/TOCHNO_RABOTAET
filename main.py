import logging
import os
import sys

from services.clustering_service import main as clustering_service
from services.dedup_service import main as dedup_service
from services.embedding_service import main as embedding_service
from services.nlp_service import main as nlp_service
from services.prefilter_service import main as prefilter_service

SERVICE_RUNNERS = {
    "prefilter": prefilter_service.run,
    "dedup": dedup_service.run,
    "embedding": embedding_service.run,
    "clustering": clustering_service.run,
    "nlp": nlp_service.run,
}


def main():
    logging.basicConfig(level=logging.INFO)
    service_name = os.getenv("SERVICE_STAGE", "prefilter")
    runner = SERVICE_RUNNERS.get(service_name)
    if runner is None:
        logging.fatal("Unknown service stage %s", service_name)
        sys.exit(1)
    runner()


if __name__ == "__main__":
    main()
