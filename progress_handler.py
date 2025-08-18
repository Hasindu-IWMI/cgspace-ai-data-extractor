import time
import logging
import json
import re
from config import Config
from api_client import APIClient
from pdf_processor import PDFProcessor
from utils import interruptable_sleep
import streamlit as st

class ProgressHandler:
    def __init__(self, config):
        self.config = config

    def check_timeout(self):
        if self.config.processing_start_time and time.time() - self.config.processing_start_time > self.config.MAX_TOTAL_PROCESSING_TIME:
            logging.error("Maximum processing time exceeded. Stopping.")
            return True
        return False

    def process_single_item_safe(self, item, page_num, driver, prompt, features, ai_provider, ai_model, progress_queue, extract_ai, selected_base_fields, ai_handler):
        if not self.config.running_event.is_set():
            logging.info("Skipping item due to stop signal")
            progress_queue.put("Skipping item due to stop signal")
            return None
        try:
            item_data, bitstreams, metadata = APIClient(self.config).get_item_details_safe(item, driver)
            if not item_data or not metadata:
                logging.warning(f"Item on page {page_num} skipped: No item data or metadata")
                progress_queue.put(f"Item on page {page_num} skipped: No item data or metadata")
                return None
            item_id = item_data.get("uuid")
            # Validate item_id
            if not isinstance(item_id, str):
                logging.error(f"Invalid item_id type for item on page {page_num}: {type(item_id)}, value: {item_id}")
                if isinstance(item_id, dict):
                    item_id = item_id.get("value", item_id.get("uuid"))
                    if not isinstance(item_id, str):
                        logging.error(f"Could not extract valid string UUID from item_id: {item_id}")
                        progress_queue.put(f"Error processing item on page {page_num}: Invalid UUID")
                        return None
                else:
                    logging.error(f"Item ID is not a string or dict: {type(item_id)}")
                    progress_queue.put(f"Error processing item on page {page_num}: Invalid UUID type")
                    return None
            try:
                self.config.processed_items.add(item_id)
            except TypeError as e:
                logging.error(f"Failed to add item_id to processed_items: {item_id}, error: {e}")
                progress_queue.put(f"Error processing item {item_id}: {str(e)}")
                return None
            progress_queue.put(f"Extracted metadata for item {item_id}")
            logging.debug(f"Item data for {item_id}: {json.dumps(item_data, indent=2)[:1000]}")
            logging.debug(f"Bitstreams for {item_id}: {json.dumps(bitstreams, indent=2)[:1000]}")
            semantic_metadata = {key: default for key, _, default in features} if extract_ai else {}
            semantic_values = []  
            if extract_ai:
                for bitstream in bitstreams[:1]:
                    if not self.config.running_event.is_set():
                        logging.info(f"Stopping PDF processing for item {item_id} due to stop signal")
                        progress_queue.put(f"Stopped PDF processing for item {item_id}")
                        break
                    pdf_url = bitstream.get("_links", {}).get("content", {}).get("href")
                    if pdf_url:
                        bitstream_id = bitstream.get("uuid")
                        filename = f"{item_id}_{bitstream_id}.pdf"
                        logging.info(f"Processing PDF for item {item_id}: {pdf_url}")
                        progress_queue.put(f"Starting PDF download for item {item_id}")
                        try:
                            _, extracted_metadata = PDFProcessor(self.config).download_pdf_safe(pdf_url, filename, page_num, item_id, prompt, features, ai_provider, ai_model, progress_queue, ai_handler)
                            if extracted_metadata:
                                semantic_metadata.update(extracted_metadata)
                                logging.info(f"Successfully processed PDF for item {item_id}")
                                break
                            else:
                                logging.warning(f"PDF processing incomplete for {pdf_url}: metadata={extracted_metadata}")
                                progress_queue.put(f"PDF processing incomplete for item {item_id}")
                        except Exception as e:
                            logging.error(f"Error processing PDF {pdf_url}: {e}")
                            progress_queue.put(f"Error processing PDF for item {item_id}: {str(e)}")
                            continue
                    else:
                        logging.info(f"No content link for bitstream {bitstream.get('uuid')} in item {item_id}")
                        progress_queue.put(f"No PDF content link for item {item_id}")
                # Flatten semantic_metadata to strings (handle lists and empty)
                for key in semantic_metadata:
                    value = semantic_metadata[key]
                    if isinstance(value, list):
                        semantic_metadata[key] = "; ".join(str(v) for v in value if v) if value else "Not detected"
                    elif value is None:
                        semantic_metadata[key] = "Not detected"
                    else:
                        semantic_metadata[key] = str(value)

                for key, type_desc, default in features:
                    value = semantic_metadata.get(key, default)
                    semantic_values.append(value)
            # Flatten base_metadata to strings (handle lists and empty)
            base_metadata = []
            for field in selected_base_fields:
                value = metadata.get(self.config.FIELD_MAPPING.get(field, field), "Unknown")
                if isinstance(value, list):
                    value = "; ".join(str(v) for v in value if v) if value else "Unknown"
                elif isinstance(value, dict):
                    value = json.dumps(value) if value else "Unknown"  # Handle rare dicts
                elif value is None or value == "":
                    value = "Unknown"
                base_metadata.append(str(value))  # Ensure string

            result = base_metadata + semantic_values
            non_null_base = sum(1 for v in result[:len(base_metadata)] if v)
            logging.debug(f"Generated result with {non_null_base}/{len(base_metadata)} base fields: {result}")
            logging.info(f"Successfully processed item {item_id} with semantic metadata: {json.dumps(semantic_metadata, indent=2)[:1000]}...")
            progress_queue.put(f"Completed processing for item {item_id}")
            return result
        except Exception as e:
            logging.error(f"Error processing item on page {page_num}: {e}")
            progress_queue.put(f"Error processing item on page {page_num}: {str(e)}")
            return None

    def reset_progress_state(self):
        st.session_state.is_running = False
        st.session_state.overall_progress_value = 0
        st.session_state.overall_progress_text = "Overall Progress"
        st.session_state.pdf_progress_value = 0
        st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
        st.session_state.excel_progress_value = 0
        st.session_state.excel_progress_text = "Excel Composing Progress"
        st.session_state.total_processed = 0
        st.session_state.pdfs_downloaded = 0
        st.session_state.ai_analyses = 0
        st.session_state.current_page = 0
        st.session_state.current_item = 0
        st.session_state.total_items_on_page = 0
        st.session_state.results = []
        st.session_state.progress_queue = None
        if hasattr(st.session_state, 'process'):
            del st.session_state.process
        if hasattr(st.session_state, 'chunk_process'):
            del st.session_state.chunk_process
        self.config.processed_items.clear() 
        logging.info("Progress state reset")
        st.rerun() 