import streamlit as st
from streamlit.runtime import get_instance
from streamlit.runtime.scriptrunner import get_script_run_ctx, add_script_run_ctx
from streamlit_autorefresh import st_autorefresh
from queue import Queue, Empty
import re
import os
import math
from datetime import timedelta
from config import Config
from logger import Logger
from ai_handler import AIHandler
from api_client import APIClient
from pdf_processor import PDFProcessor
from metadata_extractor import MetadataExtractor
from excel_writer import ExcelWriter
from progress_handler import ProgressHandler
from prompt_handler import PromptHandler
from scraper_runner import ScraperRunner
from datetime import datetime
import time
import logging
import threading
import pandas as pd



class MainApp:
    def __init__(self):
        self.config = Config()
        self.config.running_event.set() 
        self.logger = Logger()
        self.ai_handler = AIHandler(self.config, "Gemini", "") 
        self.api_client = APIClient(self.config)
        self.pdf_processor = PDFProcessor(self.config)
        self.metadata_extractor = MetadataExtractor()
        self.excel_writer = ExcelWriter(self.config)
        self.progress_handler = ProgressHandler(self.config)
        self.prompt_handler = PromptHandler()
        self.scraper_runner = ScraperRunner(self.config)
        self.logger.setup_logging()

    def run(self):
        st.set_page_config(page_title="CGSpace AI Extractor", page_icon="favicon.ico", layout="wide")
        col1, col2 = st.columns([1, 5])
        with col1:
            st.image('CGIAR-logo.png', width=100)
        with col2:
            st.title("CGSpace Data Extractor & AI Analyzer")

        if 'is_running' not in st.session_state:
            st.session_state.is_running = False
            st.session_state.display_start = 1
            st.session_state.display_end = 10
            st.session_state.total_items = 0
            st.session_state.results = []
            st.session_state.total_processed = 0
            st.session_state.pdfs_downloaded = 0
            st.session_state.ai_analyses = 0
            st.session_state.current_page = 0
            st.session_state.current_item = 0
            st.session_state.total_items_on_page = 0
            st.session_state.processing_start_time = None
            st.session_state.prompt = self.prompt_handler.set_default_prompt()
            st.session_state.driver = None
            st.session_state.selenium_status = "Not Initialized"
            st.session_state.features = self.ai_handler.parse_prompt_for_features(st.session_state.prompt)
            st.session_state.ai_provider = "Gemini"
            st.session_state.progress_queue = None
            st.session_state.total_pages = 0
            st.session_state.total_samples = 0
            st.session_state.query_validated = False
            st.session_state.last_update_time = 0
            st.session_state.overall_progress_value = 0
            st.session_state.overall_progress_text = "Overall Progress"
            st.session_state.pdf_progress_value = 0
            st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
            st.session_state.excel_progress_value = 0
            st.session_state.excel_progress_text = "Excel Composing Progress"
            st.session_state.search_success = None
            st.session_state.end_page = 1
            st.session_state.selected_base_fields = ["title", "date", "author_organization", "geography_focus", "type", "source", "url"]
            st.session_state.extract_ai = True
            st.session_state.selected_affiliations = []
            st.session_state.selected_regions = []
            st.session_state.selected_countries = []
            st.session_state.is_chunk_running = False
            st.session_state.chunk_queue = None
            st.session_state.current_item_progress = 0.0
            st.session_state.current_item_text = "Current Item Progress"
            st.session_state.current_item_id = None
            st.session_state.current_phase = None
            st.session_state.total_chunk_pages = 0
            st.session_state.current_chunk_page = 0
            st.session_state.stopping = False

            st.session_state.prev_query = ""
            st.session_state.prev_start_page = 1
            st.session_state.prev_end_page = 1
            st.session_state.prev_min_year = 2000
            st.session_state.prev_max_year = 2025
            st.session_state.prev_affiliations = []
            st.session_state.prev_regions = []
            st.session_state.prev_countries = []

        default_gemini_api_key = os.getenv("GEMINI_API_KEY", "")
        default_chatgpt_api_key = os.getenv("OPENAI_API_KEY", "")
        try:
            default_gemini_api_key = st.secrets["GEMINI_API_KEY"]
            default_chatgpt_api_key = st.secrets["OPENAI_API_KEY"]
        except (KeyError, FileNotFoundError):
            pass

        with st.sidebar:
            st.header("Extraction Options")
            st.session_state.selected_base_fields = st.multiselect(
                "Select Base Metadata Fields",
                options=["title", "date", "author_organization", "geography_focus", "type", "source", "url", "abstract", "language", "doi", "place", "coverage.region", "coverage.country", "contributor.affiliation", "creator.id", "authorship.types", "journal", "volume", "issue", "isbn", "subject.impactArea", "subject.actionArea", "contributor.donor", "contributor.project", "contributor.initiative", "file.mimetype", "file.filename", "identifier.citation"],
                default=st.session_state.selected_base_fields
            )
            st.session_state.extract_ai = st.toggle("Extract Metadata + AI Analyzed Data", value=st.session_state.extract_ai, help="Toggle off to extract only metadata")
            st.header("Upload Extraction Prompts")
            uploaded_file = st.file_uploader("Upload Excel with Prompts", type="xlsx")
            if uploaded_file:
                try:
                    df = pd.read_excel(uploaded_file)
                    prompts = {}
                    for _, row in df.iterrows():
                        pid = row.get('Prompt ID')
                        ptext = row.get('Prompt Text')
                        if pid and ptext:
                            prompts[pid] = ptext
                    if prompts:
                        st.session_state.prompt = self.prompt_handler.generate_combined_prompt(prompts)
                        st.session_state.features = self.ai_handler.parse_prompt_for_features(st.session_state.prompt)
                        normalized_base = {self.excel_writer.normalize_col_name(f).lower() for f in st.session_state.selected_base_fields}
                        filtered_features = [f for f in st.session_state.features if self.excel_writer.normalize_col_name(f[0]).lower() not in normalized_base]
                        if len(filtered_features) < len(st.session_state.features):
                            removed = len(st.session_state.features) - len(filtered_features)
                            st.warning(f"Removed {removed} duplicate features that match base metadata fields (prioritizing metadata).")
                        st.session_state.features = filtered_features
                        st.subheader("Parsed Features")
                        for key, type_desc, default in st.session_state.features:
                            st.write(f"- {key} ({type_desc}, default: {default})")
                    else:
                        st.warning("No valid prompts found in Excel.")
                except Exception as e:
                    st.error(f"Error reading Excel: {e}")
            else:
                st.info("Please upload the Excel file with prompts.")
            st.header("Settings")
            st.session_state.ai_provider = st.radio("AI Provider", ["Gemini", "ChatGPT"])
            api_key = default_gemini_api_key if st.session_state.ai_provider == "Gemini" else default_chatgpt_api_key
            self.ai_handler = AIHandler(self.config, st.session_state.ai_provider, api_key)  
            chunk_size = st.number_input("Chunk Size", value=self.config.CHUNK_SIZE, min_value=500)
            max_requests = st.number_input("Max Parallel Requests", value=self.config.MAX_PARALLEL_GEMINI_REQUESTS, min_value=1)
            request_delay = st.number_input("Request Delay (seconds)", value=self.config.REQUEST_DELAY, min_value=0.1)
            excel_file = st.text_input("Excel Output File", value=self.config.EXCEL_FILE)

            col1, col2 = st.columns(2)
            with col1:
                if st.button("Apply Settings"):
                    try:
                        self.config.CHUNK_SIZE = int(chunk_size)
                        self.config.MAX_PARALLEL_GEMINI_REQUESTS = int(max_requests)
                        self.config.REQUEST_DELAY = float(request_delay)
                        self.config.EXCEL_FILE = excel_file
                        
                        self.config.current_gemini_requests = threading.Semaphore(self.config.MAX_PARALLEL_GEMINI_REQUESTS)
                        self.config.request_count_lock = threading.Lock()
                        self.config.active_requests = 0 
                        st.success("Settings applied successfully!")
                        logging.info(f"Settings updated successfully. Features: {[f[0] for f in st.session_state.features]}")
                    except ValueError as e:
                        st.error(f"Invalid setting value: {e}")
        tabs_head = st.tabs(["Search Configuration", "Help"])
        with tabs_head[0]:
            st.subheader("Search Configuration")
            query = st.text_input("Search Query", value="", placeholder="Enter keywords to search CGSpace")
            col1, col2 = st.columns(2)
            with col1:
                start_page = st.number_input("Start Page", value=1, min_value=1)
            with col2:
                end_page = st.number_input("End Page", min_value=1, key="end_page")
            col3, col4 = st.columns(2)
            with col3:
                min_year_input = st.number_input(
                    "Min Date Issued",
                    value=2000,
                    min_value=2000,
                    max_value=2025,
                    step=1
                )
            with col4:
                max_year_input = st.number_input(
                    "Max Date Issued",
                    value=2025,
                    min_value=2000,
                    max_value=2025,
                    step=1
                )
            selected_affiliations = st.multiselect("Filter by Affiliations", self.config.AFFILIATIONS, default=st.session_state.selected_affiliations)
            st.session_state.selected_affiliations = selected_affiliations
            selected_regions = st.multiselect("Filter by Regions", self.config.REGIONS, default=st.session_state.selected_regions)
            st.session_state.selected_regions = selected_regions
            selected_countries = st.multiselect("Filter by Countries", self.config.COUNTRIES, default=st.session_state.selected_countries)
            st.session_state.selected_countries = selected_countries

            if (query != st.session_state.prev_query or
                start_page != st.session_state.prev_start_page or
                end_page != st.session_state.prev_end_page or
                min_year_input != st.session_state.prev_min_year or
                max_year_input != st.session_state.prev_max_year or
                selected_affiliations != st.session_state.prev_affiliations or
                selected_regions != st.session_state.prev_regions or
                selected_countries != st.session_state.prev_countries):
                st.session_state.query_validated = False
                st.session_state.prev_query = query
                st.session_state.prev_start_page = start_page
                st.session_state.prev_end_page = end_page
                st.session_state.prev_min_year = min_year_input
                st.session_state.prev_max_year = max_year_input
                st.session_state.prev_affiliations = selected_affiliations.copy()
                st.session_state.prev_regions = selected_regions.copy()
                st.session_state.prev_countries = selected_countries.copy()

            if 'previous_extract_ai' not in st.session_state:
                st.session_state.previous_extract_ai = st.session_state.extract_ai
            if st.session_state.extract_ai != st.session_state.previous_extract_ai:
                st.session_state.previous_extract_ai = st.session_state.extract_ai
                self.progress_handler.reset_progress_state()

            min_year = None if min_year_input == 2000 else str(min_year_input)
            max_year = None if max_year_input == 2025 else str(max_year_input)
            if min_year and max_year and int(min_year) > int(max_year):
                st.warning("Min year should not exceed max year; results may be empty.")

            def on_search():
                try:
                    total_items = self.api_client.get_total_count(query, min_year, max_year, selected_affiliations, selected_regions, selected_countries)
                    st.session_state.total_samples = total_items
                    st.session_state.total_items = total_items
                    st.session_state.total_pages = math.ceil(total_items / 10) if total_items > 0 else 0
                    st.session_state.query_validated = True
                    st.session_state.display_start = 1
                    st.session_state.display_end = min(10, total_items)
                    # Do not call reset_progress_state here to avoid resetting during search
                    filter_info = f"with affiliations: {', '.join(selected_affiliations)}" if selected_affiliations else "without affiliation filters"
                    filter_info += f", regions: {', '.join(selected_regions)}" if selected_regions else ""
                    filter_info += f", countries: {', '.join(selected_countries)}" if selected_countries else ""
                    if not query:
                        st.session_state.search_success = f"Query validated: Extracting all {total_items} documents matching filters {filter_info} across {st.session_state.total_pages} pages."
                    else:
                        st.session_state.search_success = f"Query validated: {total_items} samples found across {st.session_state.total_pages} pages {filter_info}. You can adjust start/end pages as needed."
                    logging.info(f"API response totalElements: {total_items} for query '{query}' with filters (min_year={min_year}, max_year={max_year}, affiliations={selected_affiliations}, regions={selected_regions}, countries={selected_countries})")
                except Exception as e:
                    st.session_state.search_success = f"Error validating query: {str(e)}"
                    logging.error(f"Search query failed: {str(e)}")

            st.button("Search", on_click=on_search)

            if st.session_state.search_success:
                if "Error" in st.session_state.search_success or "Please enter a search query" in st.session_state.search_success:
                    st.error(st.session_state.search_success)
                else:
                    st.success(st.session_state.search_success)
                st.session_state.search_success = None

            totals_text = st.empty()
            if st.session_state.query_validated:
                totals_text.text(f"Total pages: {st.session_state.total_pages} | Total samples: {st.session_state.total_samples}")

            search_result_text = st.empty()
            if st.session_state.query_validated:
                search_result_text.text(f"Extracting {st.session_state.display_start} - {st.session_state.display_end} of {st.session_state.total_items}")

            if st.session_state.query_validated:
                tabs = st.tabs(["Search & Process", "Extract Chunks"])
                with tabs[0]:
                    col3, col4 = st.columns(2)
                    with col3:
                        start_button = st.button("Start Processing", disabled=st.session_state.is_running or not st.session_state.query_validated)
                    with col4:
                        stop_button = st.button("Stop Processing", disabled=not st.session_state.is_running)

                    if not st.session_state.is_running and os.path.exists(self.config.EXCEL_FILE):
                        st.subheader("Download Results")
                        with open(self.config.EXCEL_FILE, "rb") as f:
                            st.download_button(
                                label="Download Excel File",
                                data=f,
                                file_name=self.config.EXCEL_FILE,
                                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                key="download_excel_file"
                            )
                    elif not st.session_state.is_running and st.session_state.total_processed > 0:
                        st.info("Processing complete, but Excel file not found.")

                    if start_button:
                        if end_page < start_page:
                            st.error("Invalid input. Ensure end page >= start_page.")
                        elif st.session_state.extract_ai and not st.session_state.features:
                            st.error("Please upload and parse the prompt Excel before starting.")
                        elif not st.session_state.query_validated:
                            st.error("Please validate the query using the Search button first.")
                        elif not st.session_state.selected_base_fields:
                            st.error("Please select at least one base metadata field.")
                        else:
                            total_to_process = max(0, min(st.session_state.total_samples - (start_page - 1) * 10, (end_page - start_page + 1) * 10))
                            st.session_state.is_running = True
                            self.config.running_event.set()
                            # self.heartbeat_monitor()
                            st.session_state.processing_start_time = time.time()
                            
                            st.session_state.progress_queue = Queue()
                            st.session_state.total_to_process = total_to_process
                            st.session_state.total_processed = 0
                            st.session_state.overall_progress_value = 0
                            st.session_state.overall_progress_text = "Overall Progress"
                            st.session_state.pdf_progress_value = 0
                            st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
                            st.session_state.excel_progress_value = 0
                            st.session_state.excel_progress_text = "Excel Composing Progress"
                            st.session_state.pdfs_downloaded = 0
                            st.session_state.ai_analyses = 0
                            st.session_state.results = []
                            extract_ai = st.session_state.extract_ai
                            selected_base_fields = st.session_state.selected_base_fields.copy()
                            selected_affiliations = st.session_state.selected_affiliations.copy()
                            selected_regions = st.session_state.selected_regions.copy()
                            selected_countries = st.session_state.selected_countries.copy()
                            process_thread = threading.Thread(
                                target=self.scraper_runner.run_scraper,
                                args=(query, start_page, end_page, api_key, None, [], 0, 0, 0, st.session_state.prompt, st.session_state.features, st.session_state.ai_provider, st.session_state.total_pages, total_to_process, min_year, max_year, selected_affiliations, selected_regions, selected_countries, st.session_state.progress_queue, extract_ai, selected_base_fields),
                                daemon=True
                            )
                            ctx = get_script_run_ctx()
                            if ctx:
                                add_script_run_ctx(process_thread, ctx)
                            process_thread.start()
                            st.session_state.process_thread = process_thread
                            st.session_state.last_update_time = time.time()
                            st.rerun()

                    if stop_button:
                        st.session_state.stopping = True
                        self.config.running_event.clear()
                        if hasattr(st.session_state, 'process_thread') and st.session_state.process_thread.is_alive():
                            if st.session_state.driver:
                                try:
                                    st.session_state.driver.quit()
                                    st.session_state.driver = None
                                    logging.info("Selenium driver closed during stop")
                                except Exception as e:
                                    logging.error(f"Error closing driver: {e}")
                            st.session_state.process_thread.join(timeout=5)
                            if st.session_state.process_thread.is_alive():
                                logging.warning("Threads did not stop gracefully")
                        self.progress_handler.reset_progress_state() 
                        st.session_state.is_running = False
                        st.session_state.stopping = False
                        st.rerun()

                    status_placeholder = st.empty()
                    if st.session_state.is_running:
                        with status_placeholder.status("Processing Status", expanded=True) as status:
                            
                            def update_progress():
                                if st.session_state.progress_queue is None:
                                    return
                                update_received = False
                                try:
                                    while True:
                                        update = st.session_state.progress_queue.get_nowait()
                                        logging.info(f"Queue update: {update}")
                                        if update is None:
                                            st.session_state.is_running = False
                                            self.progress_handler.reset_progress_state()
                                            break
                                        elif "Stopped" in update:
                                            st.session_state.is_running = False
                                            self.progress_handler.reset_progress_state()
                                            break
                                        elif "Error:" in update:
                                            st.session_state.is_running = False
                                            self.progress_handler.reset_progress_state()
                                            break
                                        elif isinstance(update, dict) and update.get('type') == 'final':
                                            st.session_state.is_running = False
                                            self.progress_handler.reset_progress_state()
                                            break
                                        else:
                                            if "Total samples" in update:
                                                st.session_state.total_to_process = int(update.split(": ")[1])
                                                st.session_state.total_processed = 0
                                                st.session_state.overall_progress_text = f"Overall Progress: 0% (0/{st.session_state.total_to_process} items)"
                                            elif "Display range" in update:
                                                st.session_state.display_start = int(update.split(": ")[1].split(" - ")[0])
                                                st.session_state.display_end = int(update.split(" - ")[1].split(" of ")[0])
                                                st.session_state.total_items = int(update.split(" of ")[1])
                                                st.session_state.overall_progress_text = f"Extracting {st.session_state.display_start} - {st.session_state.display_end} of {st.session_state.total_items}"
                                            elif "Item completed" in update:
                                                parts = update.split(": ")[1].split("/")
                                                total_processed = int(parts[0])
                                                total_items = int(parts[1])
                                                st.session_state.total_processed = total_processed
                                                overall_value = total_processed / total_items if total_items > 0 else 0
                                                st.session_state.overall_progress_value = min(overall_value, 1.0)
                                                st.session_state.overall_progress_text = f"Overall Progress: {int(overall_value * 100)}% ({total_processed}/{total_items} items)"
                                            elif "Overall progress" in update:
                                                percent = int(update.split(": ")[1].split("%")[0])
                                                st.session_state.overall_progress_value = min(percent / 100.0, 1.0)
                                                st.session_state.overall_progress_text = f"Overall Progress: {percent}% ({st.session_state.total_processed}/{st.session_state.total_to_process} items)"
                                            elif "Processing item" in update:
                                                current_item = int(update.split("item ")[1].split(" ")[0])
                                                total_items = int(update.split("of ")[1].split(" ")[0])
                                                st.session_state.current_item = current_item
                                                st.session_state.total_items_on_page = total_items
                                                st.session_state.pdf_progress_value = 0
                                                st.session_state.pdf_progress_text = f"PDF Data Extraction Progress: Item {current_item}/{total_items}"
                                            elif "Extracted metadata for item" in update:
                                                st.session_state.current_item_id = update.split("item ")[1]
                                                st.session_state.current_phase = 'metadata'
                                                st.session_state.current_item_progress = 0.1
                                                st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Metadata Extracted"
                                            elif "Starting PDF download for item" in update:
                                                st.session_state.current_item_id = update.split("item ")[1]
                                                st.session_state.current_phase = 'download_start'
                                                st.session_state.pdf_progress_value = 0.0
                                                st.session_state.pdf_progress_text = f"PDF Download Started for {st.session_state.current_item_id}"
                                                st.session_state.current_item_progress = 0.2
                                                st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Starting Download"
                                            elif "Downloading PDF" in update:
                                                if "%" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0])
                                                    st.session_state.pdf_progress_value = min(percent / 100.0 * 0.25, 0.25)
                                                    st.session_state.pdf_progress_text = f"PDF Data Extraction: Download {percent}%"
                                                else:
                                                    st.session_state.pdf_progress_value = 0.125
                                                    st.session_state.pdf_progress_text = "PDF Data Extraction: Downloading"
                                            elif "Download completed" in update:
                                                st.session_state.pdf_progress_value = 0.25
                                                st.session_state.pdf_progress_text = "PDF Data Extraction: Download Completed"
                                            elif "Extracting text" in update:
                                                if "%" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0])
                                                    st.session_state.pdf_progress_value = 0.25 + min(percent / 100.0 * 0.25, 0.25)
                                                    st.session_state.pdf_progress_text = f"PDF Data Extraction: Extraction {percent}%"
                                                else:
                                                    st.session_state.pdf_progress_value = 0.375
                                                    st.session_state.pdf_progress_text = "PDF Data Extraction: Extracting"
                                            elif "Extraction completed" in update:
                                                st.session_state.pdf_progress_value = 0.5
                                                st.session_state.pdf_progress_text = "PDF Data Extraction: Extraction Completed"
                                            elif "Chunking text" in update:
                                                if "%" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0])
                                                    st.session_state.pdf_progress_value = 0.5 + min(percent / 100.0 * 0.1, 0.1)
                                                    st.session_state.pdf_progress_text = f"PDF Data Extraction: Chunking {percent}%"
                                                else:
                                                    st.session_state.pdf_progress_value = 0.55
                                                    st.session_state.pdf_progress_text = "PDF Data Extraction: Chunking"
                                            elif "Chunking completed" in update:
                                                st.session_state.pdf_progress_value = 0.6
                                                st.session_state.pdf_progress_text = "PDF Data Extraction: Chunking Completed"
                                            elif "Analyzing chunk" in update:
                                                chunk_num = int(update.split("chunk ")[1].split("/")[0])
                                                total_chunks = int(update.split("/")[1].split(" ")[0])
                                                progress_val = min(chunk_num / total_chunks, 1.0)
                                                st.session_state.pdf_progress_value = 0.6 + progress_val * 0.4
                                                st.session_state.pdf_progress_text = f"PDF Data Extraction: Analysis Chunk {chunk_num}/{total_chunks} ({int(progress_val * 100)}%)"
                                            elif "Analysis completed" in update:
                                                st.session_state.pdf_progress_value = 1.0
                                                st.session_state.pdf_progress_text = "PDF Data Extraction: Completed"
                                            elif "Completed processing for item" in update:
                                                st.session_state.total_processed += 1 
                                                if st.session_state.total_to_process > 0:
                                                    overall_value = st.session_state.total_processed / st.session_state.total_to_process if st.session_state.total_to_process > 0 else 0
                                                    st.session_state.overall_progress_value = min(overall_value, 1.0)
                                                    st.session_state.overall_progress_text = f"Overall Progress: {int(overall_value * 100)}% ({st.session_state.total_processed}/{st.session_state.total_to_process} items)"
                                                    st.session_state.current_item_progress = 1.0
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Completed"
                                                    st.session_state.excel_progress_value = min(st.session_state.total_processed / st.session_state.total_to_process, 1.0)  # Update Excel progress
                                                    st.session_state.excel_progress_text = f"Excel Composing: {int(st.session_state.excel_progress_value * 100)}%"
                                            elif "Composing Excel" in update:
                                                if "%" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0])
                                                    st.session_state.excel_progress_value = min(percent / 100.0, 1.0)
                                                    st.session_state.excel_progress_text = f"Excel Composing: {percent}%"
                                            elif "Excel composition completed" in update:
                                                st.session_state.excel_progress_value = 1.0
                                                st.session_state.excel_progress_text = "Excel Composing: Completed"
                                            elif "Processing completed" in update or "No items found" in update:
                                                st.session_state.is_running = False
                                                st.session_state.overall_progress_value = 1.0
                                                st.session_state.overall_progress_text = "Overall Progress: Completed"
                                                st.session_state.pdf_progress_value = 0
                                                st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
                                                st.session_state.excel_progress_value = 0
                                                st.session_state.excel_progress_text = "Excel Composing Progress"
                                                status.update(label="Processing completed!", state="complete", expanded=False)
                                                break
                                            else:
                                                logging.warning(f"Unmatched progress update: {update}")
                                            update_received = True
                                            
                                except Empty:
                                    pass
                                except Exception as e:
                                    logging.warning(f"Fragment error in update_chunk_progress: {str(e)}")
                                    return
                                if update_received:
                                    st.rerun()  # Full rerun if new updates to ensure state sync
                                st.progress(st.session_state.overall_progress_value, text=st.session_state.overall_progress_text)
                                st.progress(st.session_state.pdf_progress_value, text=st.session_state.pdf_progress_text)
                                st.progress(st.session_state.excel_progress_value, text=st.session_state.excel_progress_text)
                                elapsed = time.time() - st.session_state.processing_start_time
                                st.text(f"Elapsed Time: {timedelta(seconds=elapsed)}")
                                st_autorefresh(interval=2000, key="progress_refresh")  # Backup

                            update_progress()

                            st.subheader("Quick Actions")
                            if st.button("Download Excel", key="download_excel"):
                                if os.path.exists(self.config.EXCEL_FILE):
                                    with open(self.config.EXCEL_FILE, "rb") as f:
                                        st.download_button(
                                            label="Download Excel File",
                                            data=f,
                                            file_name=self.config.EXCEL_FILE,
                                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                            key="download_excel_file"
                                        )
                                else:
                                    st.info("Excel file doesn't exist yet. Start processing to create it.")

                with tabs[1]:
                    col_chunk1, col_chunk2 = st.columns(2)
                    with col_chunk1:
                        chunk_start_button = st.button("Extract Chunk Data", disabled=st.session_state.is_running or not st.session_state.query_validated or st.session_state.get('is_chunk_running', False))
                    with col_chunk2:
                        chunk_stop_button = st.button("Stop Chunk Extraction", disabled=not st.session_state.get('is_chunk_running', False))

                    if chunk_start_button:
                        if not st.session_state.selected_base_fields:
                            st.error("Select base metadata fields to include in chunks.")
                        else:
                            st.session_state.is_chunk_running = True
                            self.config.running_event.set()
                            st.session_state.chunk_queue = Queue()
                            st.session_state.current_item_progress = 0.0
                            st.session_state.current_item_text = "Current Item Progress"
                            st.session_state.overall_progress_value = 0.0
                            st.session_state.overall_progress_text = "Overall Progress"
                            st.session_state.current_item_id = None
                            st.session_state.current_phase = None
                            st.session_state.total_chunk_pages = end_page - start_page + 1
                            st.session_state.current_chunk_page = start_page
                            chunk_thread = threading.Thread(
                                target=self.scraper_runner.run_chunk_extraction,
                                args=(query, start_page, end_page, None, st.session_state.selected_base_fields, min_year, max_year, st.session_state.selected_affiliations, st.session_state.selected_regions, st.session_state.selected_countries, st.session_state.chunk_queue),
                                daemon=True
                            )
                            chunk_thread.start()
                            st.session_state.chunk_thread = chunk_thread
                            st.rerun()

                    if chunk_stop_button:
                        self.config.running_event.clear()
                        st.session_state.is_chunk_running = False
                        if 'chunk_queue' in st.session_state and st.session_state.chunk_queue is not None:
                            st.session_state.chunk_queue.put("Stopped chunk extraction.")
                        logging.info("Chunk extraction stopped by user")
                        self.progress_handler.reset_progress_state()
                        if hasattr(st.session_state, 'chunk_thread') and st.session_state.chunk_thread.is_alive():
                            # Threads set event and wait
                            st.session_state.chunk_thread.join(timeout=5)  # Optional timeout
                        st.rerun()

                    chunk_status = st.empty()
                    if 'chunk_queue' in st.session_state and st.session_state.chunk_queue is not None:
                        with chunk_status.container():
                            with st.status("Chunk Extraction Status", expanded=True) as chunk_stat:
                                def update_chunk_progress():
                                    if st.session_state.chunk_queue is None:
                                        return
                                    update_received = False
                                    while True:
                                        try:
                                            update = st.session_state.chunk_queue.get_nowait()
                                            logging.info(f"Chunk queue update: {update}")
                                            if update is None:
                                                st.session_state.is_chunk_running = False
                                                self.progress_handler.reset_progress_state()
                                                st.rerun()
                                                break
                                            elif "Stopped chunk extraction." in update:
                                                st.session_state.is_chunk_running = False
                                                self.progress_handler.reset_progress_state()
                                                st.rerun()
                                                break
                                            elif "Error:" in update:
                                                st.session_state.is_chunk_running = False
                                                self.progress_handler.reset_progress_state()
                                                st.rerun()
                                                break
                                            elif isinstance(update, dict) and update.get('type') == 'final':
                                                st.session_state.is_chunk_running = False
                                                self.progress_handler.reset_progress_state()
                                                st.rerun()
                                                break
                                            else:
                                                if "Total pages for chunking" in update:
                                                    st.session_state.total_chunk_pages = int(update.split(": ")[1])
                                                elif "Processing page" in update:
                                                    st.session_state.current_chunk_page = int(update.split("page ")[1].split(" ")[0])
                                                    overall_progress = (st.session_state.current_chunk_page - start_page) / (end_page - start_page + 1)
                                                    st.session_state.overall_progress_value = min(overall_progress, 1.0)
                                                    st.session_state.overall_progress_text = f"Overall Progress: Page {st.session_state.current_chunk_page - start_page + 1}/{st.session_state.total_chunk_pages}"
                                                    st.session_state.current_item_progress = 0
                                                    st.session_state.current_item_text = "Current Item Progress"
                                                elif "Chunking document" in update:
                                                    st.session_state.current_item_id = update.split(" ")[2] if len(update.split(" ")) > 2 else "Unknown"
                                                    st.session_state.current_phase = 'start'
                                                    st.session_state.current_item_progress = 0.0
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Starting"
                                                elif "Downloading PDF" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0]) if "%" in update else 50
                                                    st.session_state.current_phase = 'download'
                                                    st.session_state.current_item_progress = percent / 100.0 * 0.3
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Download {percent}%"
                                                elif "Download completed" in update:
                                                    st.session_state.current_item_progress = 0.3
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Download Completed"
                                                elif "Extracting text from PDF" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0]) if "%" in update else 50
                                                    st.session_state.current_phase = 'extract'
                                                    st.session_state.current_item_progress = 0.3 + (percent / 100.0 * 0.4)
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Extraction {percent}%"
                                                elif "Extraction completed" in update:
                                                    st.session_state.current_item_progress = 0.7
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Extraction Completed"
                                                elif "Chunking text" in update:
                                                    percent = int(update.split(": ")[1].split("%")[0]) if "%" in update else 50
                                                    st.session_state.current_phase = 'chunk'
                                                    st.session_state.current_item_progress = 0.7 + (percent / 100.0 * 0.2)
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Chunking {percent}%"
                                                elif "Chunking completed" in update:
                                                    st.session_state.current_item_progress = 0.9
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Chunking Completed"
                                                elif "Chunked" in update:
                                                    st.session_state.current_item_progress = 1.0
                                                    st.session_state.current_item_text = f"Current Item {st.session_state.current_item_id}: Completed"
                                                elif any(word in update.lower() for word in ["stopped", "stopping", "stop signal"]):
                                                    st.session_state.is_chunk_running = False
                                                    self.progress_handler.reset_progress_state()
                                                    break
                                                elif "No PDF found for" in update:
                                                    # Handle no PDF case - optional progress or text update
                                                    st.session_state.current_item_text = f"No PDF found for item, skipping"
                                                elif "Chunks saved to" in update:
                                                    # Handle save completion
                                                    st.session_state.overall_progress_value = 1.0
                                                    st.session_state.overall_progress_text = "Chunk Extraction Completed"
                                                else:
                                                    logging.warning(f"Unmatched chunk progress update: {update}")
                                                update_received = True
                                        except Empty:
                                            break
                                    if update_received:
                                        st.rerun()
                                    st.progress(st.session_state.overall_progress_value, text=st.session_state.overall_progress_text)
                                    st.progress(st.session_state.current_item_progress, text=st.session_state.current_item_text)
                                    st_autorefresh(interval=2000, key="chunk_refresh")

                                update_chunk_progress()
                                chunk_progress_text = st.empty()
                    if os.path.exists("cgspace_chunks_data.xlsx"):
                        with open("cgspace_chunks_data.xlsx", "rb") as f:
                            st.download_button("Download Chunks Excel", data=f, file_name="cgspace_chunks_data.xlsx")
            else:
                st.info("Please perform a search to enable processing and chunk extraction.")

        with tabs_head[1]:
            st.subheader("Help & Documentation")
            st.markdown("""
                Welcome to the **CGSpace Data Extractor & AI Analyzer**, a tool for extracting and analyzing metadata and AI-generated insights from CGSpace publications. Follow these steps to use the application effectively:

                ### 1. Configure Settings (Sidebar)
                1. **Extraction Options**:
                - **Base Metadata Fields**: Select metadata fields (e.g., title, date, author) to extract from CGSpace. Choose at least one field.
                - **Extract Metadata + AI Analyzed Data**: Toggle ON to include AI-generated insights (requires an uploaded prompt Excel). Toggle OFF for metadata-only extraction.
                - **Upload Extraction Prompts**:
                - Upload an Excel file (`AI-CoScientist-Prompt.xlsx`) with prompts for AI analysis. The file should have columns: `Prompt ID` and `Prompt Text`.
                - Example prompts: "Year", "Country", "SDGs", "Lead Authors". Ensure prompts specify output formats (e.g., semicolon-separated strings).
                2. **Settings**:
                - **AI Provider**: Choose "Gemini" or "ChatGPT" for AI analysis.
                - **Chunk Size**: Set the text chunk size for AI processing (default: 500 characters).
                - **Max Parallel Requests**: Set the number of simultaneous AI requests (default: based on config).
                - **Request Delay**: Set the delay between AI requests (default: 0.1 seconds).
                - **Excel Output File**: Specify the output Excel file name (default: `cgspace_semantic_data.xlsx`).
                - Click **Apply Settings** to save changes.

                ### 2. Search for Publications
                - **Search Configuration**:
                - **Search Query**: Enter keywords to search CGSpace (e.g., "machine learning agriculture"). Leave blank for all documents.
                - **Start Page / End Page**: Specify the page range for results (each page contains up to 10 items).
                - **Min/Max Date Issued**: Filter by publication year (default: 2000–2025).
                - **Affiliations/Regions/Countries**: Select filters to narrow results (optional).
                - Click **Search** to validate the query and display the total items and pages found.
                - **Note**: If you change any search field (query, dates, filters), you must click **Search** again to revalidate.

                ### 3. Process Data (Search & Process Tab)
                - After a successful search, the **Search & Process** tab becomes active.
                - Click **Start Processing** to extract metadata and (if enabled) AI-analyzed data for the selected page range.
                - Monitor progress via the status bars:
                - **Overall Progress**: Shows total items processed.
                - **PDF Data Extraction Progress**: Tracks PDF download and extraction.
                - **Excel Composing Progress**: Tracks Excel file creation.
                - Click **Stop Processing** to halt the process.
                - Once complete, download the results via the **Download Excel File** button (saved as `cgspace_semantic_data.xlsx`).

                ### 4. Extract Chunks (Extract Chunks Tab)
                - After a search, use the **Extract Chunks** tab to extract text chunks from PDFs with metadata.
                - Click **Extract Chunk Data** to start chunk extraction.
                - Monitor progress via status bars.
                - Click **Stop Chunk Extraction** to halt.
                - Download results via the **Download Chunks Excel** button (saved as `cgspace_chunks_data.xlsx`).

                ### 5. Tips for Best Results
                - **Prompt Excel**: Ensure prompts are clear and specify output formats (e.g., "semicolon-separated string" for lists).
                - **Search Validation**: Always re-run **Search** after changing inputs to ensure correct parameters.
                - **Error Handling**: Check logs in the console for issues (e.g., PDF access errors). Contact CGSpace admins if many PDFs are restricted.

                For support, contact [h.ramanayake@cgiar.org].
                            """
            )
            

if __name__ == "__main__":
    app = MainApp()
    app.run()