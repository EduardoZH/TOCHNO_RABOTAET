import importlib
import logging
import os
import sys

SERVICE_MODULES = {
    "splitter": "services.splitter_service.main",
    "prefilter": "services.prefilter_service.main",
    "dedup": "services.dedup_service.main",
    "embedding": "services.embedding_service.main",
    "clustering": "services.clustering_service.main",
    "nlp": "services.nlp_service.main",
    "aggregator": "services.aggregator_service.main",
}


def main():
    logging.basicConfig(level=logging.INFO)
    service_name = os.getenv("SERVICE_STAGE", "prefilter")
    module_path = SERVICE_MODULES.get(service_name)
    if module_path is None:
        logging.fatal("Unknown service stage %s", service_name)
        sys.exit(1)
    module = importlib.import_module(module_path)
    module.run()


if __name__ == "__main__":
    main()
