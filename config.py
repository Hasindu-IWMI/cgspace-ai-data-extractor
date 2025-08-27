import json
import logging
import multiprocessing as mp

class Config:
    
    EXCEL_FILE = "cgspace_extraction_data.xlsx"
    API_BASE_URL = "https://cgspace.cgiar.org/server/api"
    CHUNK_SIZE = 3000
    MAX_PARALLEL_GEMINI_REQUESTS = 2
    REQUEST_DELAY = 1.0
    CHUNK_OVERLAP = 150
    GEMINI_TIMEOUT = 60
    PDF_PROCESSING_TIMEOUT = 300
    MAX_TOTAL_PROCESSING_TIME = 36000

    def __init__(self):
        self.processed_items = set()
        self.API_TOKEN = None
        self.processing_start_time = None
        self.current_gemini_requests = mp.Semaphore(self.MAX_PARALLEL_GEMINI_REQUESTS)
        self.request_count_lock = mp.Lock()
        self.active_requests = mp.Value('i', 0)
        self.running_event = mp.Event()
        self.gemini_model = None
        self.GEMINI_AVAILABLE = False
        self.chatgpt_client = None
        self.CHATGPT_AVAILABLE = False
        self.load_mapping_config()

    def load_mapping_config(self):
        try:
            with open('config.json', 'r') as config_file:
                config = json.load(config_file)
            self.FIELD_MAPPING = config['FIELD_MAPPING']
            self.AFFILIATIONS = config['AFFILIATIONS']
            self.REGIONS = config['REGIONS']
            self.COUNTRIES = config['COUNTRIES']
        except FileNotFoundError:
            logging.error("config.json file not found")
            raise
        except json.JSONDecodeError:
            logging.error("Invalid JSON format in config.json")
            raise