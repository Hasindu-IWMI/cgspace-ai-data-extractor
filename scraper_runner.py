import logging
import time
import math
import threading
from datetime import timedelta
from config import Config
from ai_handler import AIHandler
from api_client import APIClient
from progress_handler import ProgressHandler
from excel_writer import ExcelWriter
from utils import interruptable_sleep
import os
import multiprocessing as mp



class ScraperRunner:
    def __init__(self, config):
        self.config = config

    def run_scraper(self, query, start_page, end_page, api_key, driver, results, total_processed, pdfs_downloaded, ai_analyses, prompt, features, ai_provider, total_pages, total_samples, min_year, max_year, selected_affiliations, selected_regions, selected_countries, progress_queue, extract_ai, selected_base_fields):
        results = []  # Initialize locally
        if driver is None:
            driver = APIClient(self.config).setup_selenium_driver()
            if not driver:
                progress_queue.put("Error: Failed to initialize Selenium WebDriver")
                if progress_queue:
                    progress_queue.put({'type': 'final', 'results': [], 'total_processed': 0, 'pdfs_downloaded': 0, 'ai_analyses': 0, 'progress_updates': []})
                return
        global processing_start_time, gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE
        ai_handler = None  # Initialize to None
        ai_model = None
        if extract_ai:
            ai_handler = AIHandler(self.config, ai_provider, api_key)
            gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE = AIHandler(self.config, ai_provider, api_key).initialize_ai()
            if (ai_provider == "Gemini" and not GEMINI_AVAILABLE) or (ai_provider == "ChatGPT" and not CHATGPT_AVAILABLE):
                logging.error("Cannot start processing: AI API not initialized")
                progress_queue.put("Error: AI API not initialized")
                if progress_queue:
                    progress_queue.put({'type': 'final', 'results': [], 'total_processed': 0, 'pdfs_downloaded': 0, 'ai_analyses': 0, 'progress_updates': []})
                return
            ai_model = gemini_model if ai_provider == "Gemini" else chatgpt_client
        else:
            gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE = None, False, None, False
        processing_start_time = time.time()
        if os.path.exists(self.config.EXCEL_FILE):
            os.remove(self.config.EXCEL_FILE)
            logging.info(f"Deleted existing {self.config.EXCEL_FILE} to start fresh.")
        progress_updates = []
        processed_lock = mp.Lock()
        try:
            total_items = APIClient(self.config).get_total_count(query, min_year, max_year, selected_affiliations, selected_regions, selected_countries)
            expected_samples = (end_page - start_page + 1) * 10
            total_samples = min(total_items - (start_page - 1) * 10, expected_samples)
            progress_queue.put(f"Total samples: {total_samples}")
            if total_samples <= 0:
                logging.info("No items in selected range. Stopping.")
                progress_updates.append("No items in selected range. Stopping.")
                progress_queue.put({'type': 'final', 'results': results, 'total_processed': total_processed, 'pdfs_downloaded': pdfs_downloaded, 'ai_analyses': ai_analyses, 'progress_updates': progress_updates})
                return
            total_pages = math.ceil(total_items / 10)
            for page in range(start_page, end_page + 1):
                if not self.config.running_event.is_set():
                    progress_queue.put("Stopped during page processing")
                    break
                logging.info(f"Processing page {page}...")
                page_start_time = time.time()
                items, page_total = APIClient(self.config).search_items(query, page, min_year, max_year, selected_affiliations, selected_regions, selected_countries, size=10)
                if page_total != total_items:
                    logging.warning(f"Total items mismatch: initial={total_items}, page {page} total={page_total}")
                if not items:
                    logging.info(f"No items found on page {page}.")
                    progress_updates.append(f"No items found on page {page}.")
                    continue
           
                display_start = (page - 1) * 10 + 1
                display_end = min(page * 10, total_items)
                progress_queue.put(f"Display range: {display_start} - {display_end} of {total_items}")
                total_items_on_page = len(items)
                progress_updates.append(f"Processing page {page} of {total_pages} ({total_items} total items)")
                for i, item in enumerate(items):
                    if not self.config.running_event.is_set():
                        progress_queue.put("Stopped during item processing")
                        break
                    current_item = i + 1
                    progress_updates.append(f"Processing item {current_item} of {len(items)} on page {page}")
                    logging.info(f"Processing item {current_item}/{len(items)} on page {page}")
                    item_start_time = time.time()
                    if extract_ai:
                        if ai_provider == "Gemini":
                            ai_model = gemini_model
                        else:
                            ai_model = chatgpt_client
                    else:
                        ai_model = None
                    result = ProgressHandler(self.config).process_single_item_safe(item, page, driver, prompt, features, ai_provider, ai_model, progress_queue, extract_ai, selected_base_fields, ai_handler)
                    with processed_lock:
                        if result:
                            results.append(result)
                            total_processed += 1
                            if extract_ai:
                                pdfs_downloaded += 1
                                ai_analyses += 1
                            progress_queue.put(f"Item completed: {total_processed}/{total_samples}")
                            logging.info(f"Item {current_item} completed in {time.time() - item_start_time:.2f}s")
                        else:
                            progress_queue.put(f"Item {current_item} on page {page} skipped or failed")
                            logging.warning(f"Item {current_item} processing failed or skipped")
                        overall_progress = total_processed / total_samples if total_samples > 0 else 0
                        progress_queue.put(f"Overall progress: {int(overall_progress * 100)}%")
                    interruptable_sleep(0.1, self.config.running_event)
                if results:
                    ExcelWriter(self.config).save_to_excel(results, features, progress_queue=progress_queue, extract_ai=extract_ai, selected_base_fields=selected_base_fields)
                    results = []  # Clear after saving
                page_time = time.time() - page_start_time
                logging.info(f"Page {page} completed in {page_time:.2f} seconds. Processed {total_processed} items.")
                progress_updates.append(f"Page {page} completed")
                interruptable_sleep(1, self.config.running_event)
            total_time = time.time() - processing_start_time
            logging.info(f"Processing completed! Total items: {total_processed}, Time: {total_time:.2f}s")
            progress_updates.append(f"Processing completed! Total items: {total_processed}, Time: {total_time:.2f}s")
        except Exception as e:
            logging.error(f"Unexpected error in run_scraper: {e}")
            progress_updates.append(f"Error during processing: {str(e)}")
        finally:
            if driver:
                try:
                    driver.quit()
                    logging.info("Selenium driver closed")
                except Exception as e:
                    logging.warning(f"Error closing driver: {e}")
            if results:
                ExcelWriter(self.config).save_to_excel(results, features, progress_queue=progress_queue, extract_ai=extract_ai, selected_base_fields=selected_base_fields)
            if progress_queue:
                progress_queue.put({'type': 'final', 'results': results, 'total_processed': total_processed, 'pdfs_downloaded': pdfs_downloaded, 'ai_analyses': ai_analyses, 'progress_updates': progress_updates})

    def run_chunk_extraction(self, query, start_page, end_page, driver, selected_base_fields, min_year, max_year, selected_affiliations, selected_regions, selected_countries, chunk_queue):
        logging.info("Chunk extraction thread started")
        chunk_queue.put("Starting chunk extraction...")
        if driver is None:
            driver = APIClient.setup_selenium_driver(self)
            if not driver:
                chunk_queue.put("Error: Failed to initialize Selenium WebDriver")
                chunk_queue.put({'type': 'final'})
                return
        try:
            ExcelWriter(self.config).extract_chunks_and_save(query, start_page, end_page, driver, selected_base_fields, min_year, max_year, selected_affiliations, selected_regions, selected_countries, chunk_queue)
        except Exception as e:
            logging.error(f"Error in chunk extraction: {e}")
            chunk_queue.put(f"Error: {str(e)}")
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
            chunk_queue.put({'type': 'final'})
