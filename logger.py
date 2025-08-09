import logging

class Logger:
    @staticmethod
    def setup_logging():
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("cgspace_semantic_scraper.log"),
                logging.StreamHandler()
            ]
        )