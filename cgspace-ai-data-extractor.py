import os
import time
import requests
import streamlit as st
import json
import pandas as pd
import fitz # PyMuPDF
from google.generativeai import GenerativeModel, configure
import logging
from concurrent.futures import ThreadPoolExecutor
import hashlib
import re
import threading
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import uuid
import io
import openpyxl
import openai
from queue import Queue, Empty
import tempfile
import math
from streamlit_autorefresh import st_autorefresh
# Configuration
BASE_SAVE_DIR = "cgiar_ai_pdfs"
EXCEL_FILE = "cgspace_semantic_data.xlsx"
API_BASE_URL = "https://cgspace.cgiar.org/server/api"
CHUNK_SIZE = 3000
MAX_PARALLEL_GEMINI_REQUESTS = 2
REQUEST_DELAY = 1.0
CHUNK_OVERLAP = 150
GEMINI_TIMEOUT = 60
PDF_PROCESSING_TIMEOUT = 300
MAX_TOTAL_PROCESSING_TIME = 36000
# Global variables
processed_items = set()
API_TOKEN = None
processing_start_time = None
current_gemini_requests = threading.Semaphore(MAX_PARALLEL_GEMINI_REQUESTS)
request_count_lock = threading.Lock()
active_requests = 0
gemini_model = None
GEMINI_AVAILABLE = False
chatgpt_client = None
CHATGPT_AVAILABLE = False
running_event = threading.Event()


FIELD_MAPPING = {
    "title": "dc.title",
    "date": "dcterms.issued",
    "author_organization": "dc.contributor.author",
    "geography_focus": "cg.coverage.country",
    "type": "dcterms.type",
    "source": "source",
    "url": "dc.identifier.uri",
    "abstract": "dcterms.abstract",
    "language": "dc.language.iso",
    "doi": "dc.identifier.doi",
    "place": "dc.coverage.place",
    "coverage.region": "dc.coverage.region",
    "coverage.country": "dc.coverage.country",
    "contributor.affiliation": "cg.contributor.affiliation",
    "creator.id": "dc.creator.id",
    "authorship.types": "cg.authorship.types",
    "journal": "dc.relation.ispartofseries",
    "volume": "cg.volume",
    "issue": "cg.issue",
    "isbn": "cg.isbn",
    "subject.impactArea": "cg.subject.impactArea",
    "subject.actionArea": "cg.subject.actionArea",
    "contributor.donor": "cg.contributor.donor",
    "contributor.project": "cg.contributor.project",
    "contributor.initiative": "cg.contributor.initiative",
    "file.mimetype": "dc.format.mimetype",
    "file.filename": "dc.format.filename",
    "identifier.citation": "dc.identifier.citation"
}


# Configure logging
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("cgspace_semantic_scraper.log"),
            logging.StreamHandler()
        ]
    )
# Initialize AI
def initialize_ai(ai_provider, api_key):
    if ai_provider == "Gemini":
        try:
            configure(api_key=api_key)
            gemini_model = GenerativeModel("gemini-1.5-pro")
            return gemini_model, True, None, False
        except Exception as e:
            logging.error(f"Failed to configure Gemini API: {e}")
            return None, False, None, False
    elif ai_provider == "ChatGPT":
        try:
            client = openai.OpenAI(api_key=api_key)
            return None, False, client, True
        except Exception as e:
            logging.error(f"Failed to configure ChatGPT API: {e}")
            return None, False, None, False
# Parse prompt for features
def parse_prompt_for_features(prompt):
    features = []
    try:
        json_instruction = re.search(
            r"Return\s+(?:ONLY\s+)?a\s+valid\s+JSON\s+object\s+(?:with\s+(?:these\s+)?(?:exact\s+)?keys|containing\s+the\s+following\s+keys|with\s+the\s+following\s+fields):?\s*([\s\S]*?)(?:Document\s+text:|$)",
            prompt,
            re.DOTALL | re.IGNORECASE
        )
        if not json_instruction:
            logging.warning("No JSON keys section found in prompt. Using minimal default features.")
            return [("key_information", "list", [])]
     
        keys_section = json_instruction.group(1).strip()
        key_lines = []
        for line in keys_section.split('\n'):
            line = line.strip()
            if not line or any(keyword in line.lower() for keyword in ['always', 'even if', 'do not']):
                continue
            if line.startswith('-') or line.startswith('*'):
                line = re.sub(r'^-|\*', '', line).strip()
                key_lines.append(line)
     
        for line in key_lines:
            match = re.match(r"(\w+)\s*(?:\((.*?)\)|:\s*(.*?)(?=\s|$))?", line)
            if match:
                key = match.group(1).strip()
                type_desc = (match.group(2) or match.group(3) or "string").strip().lower()
                type_desc = type_desc.replace(" or ", " ").replace(",", " ").strip()
                if "list" in type_desc:
                    default = []
                elif "string" in type_desc:
                    default = None if "null" in type_desc else ""
                elif "integer" in type_desc:
                    default = 0
                elif "float" in type_desc:
                    default = 0.0
                elif "boolean" in type_desc:
                    default = False
                else:
                    default = None
                features.append((key, type_desc, default))
     
        if not features:
            logging.warning("No valid features parsed. Using minimal default.")
            return [("key_information", "list", [])]
     
        logging.info(f"Parsed {len(features)} features from prompt: {[f[0] for f in features]}")
        return features
    except Exception as e:
        logging.error(f"Error parsing prompt for features: {e}")
        return [("key_information", "list", [])]
# Core processing functions
def setup_selenium_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    try:
        driver = webdriver.Chrome(options=chrome_options)
        return driver
    except Exception as e:
        logging.error(f"Failed to setup Chrome driver: {e}")
        logging.info("Please install ChromeDriver or use the API-only version")
        return None
def check_timeout():
    global processing_start_time
    if processing_start_time and time.time() - processing_start_time > MAX_TOTAL_PROCESSING_TIME:
        logging.error("Maximum processing time exceeded. Stopping.")
        return True
    return False
def make_api_request_safe(url, params=None, headers=None, retries=3, timeout=30, stream=False):
    if headers is None:
        headers = {"User-Agent": "CGIAR 360 integration bot; h.ramanayake@cgiar.org"}
    if API_TOKEN:
        headers["Authorization"] = f"Bearer {API_TOKEN}"
    time.sleep(REQUEST_DELAY)
    for attempt in range(retries):
        try:
            logging.info(f"Making request to {url} (attempt {attempt + 1})")
            response = requests.get(url, params=params, headers=headers, stream=stream, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout for {url} (attempt {attempt + 1})")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                wait_time = 30 * (2 ** attempt)
                logging.info(f"Rate limited. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
                continue
            logging.error(f"HTTP Error for {url}: {e}")
            raise
        except requests.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
                continue
            raise
    raise requests.exceptions.RequestException(f"Max retries reached for {url}")
def search_items(query, page, min_year=None, max_year=None, iwmi_filter=False, size=10):
    url = f"{API_BASE_URL}/discover/search/objects"
    base_query = query.strip()
    full_query = base_query # Removed allMetadata: for now; test without it
    params = {
        "query": full_query,
        "page": page - 1, # DSpace uses 0-based indexing
        "size": size,
        "sort": "score,DESC",
        "embed": "thumbnail,item/thumbnail" # Re-added to match your example
    }
 
    # Add date filter if provided
    if min_year and max_year:
        full_query += f' AND dateIssued.year:[{min_year} TO {max_year}]'
    elif min_year:
        full_query += f' AND dateIssued.year:[{min_year} TO *]'
    elif max_year:
        full_query += f' AND dateIssued.year:[* TO {max_year}]'
    params["query"] = full_query
 
    # Add IWMI affiliation filter
    if iwmi_filter:
        params["f.affiliation"] = "International Water Management Institute,equals"
 
    try:
        logging.info(f"Sending API request with params: {params}")
        response = make_api_request_safe(url, params=params)
        data = response.json()
        logging.debug(f"Full API response: {json.dumps(data, indent=2)}") # Log full response
        items = data.get("_embedded", {}).get("searchResult", {}).get("_embedded", {}).get("objects", [])
        total_items = data.get("_embedded", {}).get("searchResult", {}).get("page", {}).get("totalElements", len(items))
        if not total_items:
            logging.warning(f"No totalElements found in response for page {page}: {json.dumps(data)}")
        logging.info(f"Page {page}: Found {len(items)} items (Total: {total_items}) with filters min_year={min_year}, max_year={max_year}, iwmi_filter={iwmi_filter}")
        return items, total_items
    except requests.exceptions.HTTPError as e:
        logging.error(f"HTTP Error for {url}: {e.response.status_code} - {e.response.text}")
        return [], 0
    except Exception as e:
        logging.error(f"Failed to fetch search results for page {page}: {e}")
        return [], 0
def get_total_count(query, min_year=None, max_year=None, iwmi_filter=False):
    _, total_items = search_items(query, 1, min_year, max_year, iwmi_filter, size=1) # Use size=1 instead of 0
    logging.info(f"Total count for query '{query}' with filters (min_year={min_year}, max_year={max_year}, iwmi_filter={iwmi_filter}): {total_items}")
    return total_items
def extract_metadata_from_web(item_id, item_data, driver):
    metadata = {
        "source": "CGSpace",
        "url": f"https://cgspace.cgiar.org/items/{item_id}"
    }
    try:
        metadata_entries = item_data.get("metadata", {})
        logging.debug(f"Metadata entries for {item_id}: {metadata_entries}")
        for key, entries in metadata_entries.items():
            if entries:
                # Take the first value for simplicity; handle lists if needed
                value = entries[0].get("value", "Unknown")
                metadata[key] = value  # Use full key as-is
        if driver:
            try:
                url = f"https://cgspace.cgiar.org/items/{item_id}/full"
                logging.info(f"Scraping metadata from {url}")
                driver.get(url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                soup = BeautifulSoup(driver.page_source, "html.parser")
                table = soup.find("table")
                if table:
                    rows = table.find_all("tr")
                    for row in rows:
                        cells = row.find_all("td")
                        if len(cells) < 2:
                            continue
                        key = cells[0].text.strip()
                        value = cells[1].text.strip()
                        if key not in metadata or metadata[key] == "Unknown":
                            metadata[key] = value
                logging.debug(f"Web scraped metadata for {item_id}: {soup.prettify()[:500]}...")
            except Exception as e:
                logging.warning(f"Failed to scrape web metadata for {item_id}: {e}")
    except Exception as e:
        logging.error(f"Error processing metadata for {item_id}: {e}")
    logging.info(f"Extracted web metadata for {item_id}: {list(metadata.keys())}")
    return metadata

def extract_pdf_text_safe(pdf_path, progress_queue=None, item_id=None):
    try:
        logging.info(f"Extracting text from file {pdf_path}")
        pdf = fitz.open(pdf_path)
        text = ""
        page_count = len(pdf)
        logging.info(f"PDF has {page_count} pages")
        for page_num, page in enumerate(pdf):
            if not running_event.is_set():
                logging.info(f"Stopping PDF text extraction at page {page_num + 1} due to stop signal")
                break
            try:
                page_text = page.get_text() or ""
                if page_text.strip():
                    text += page_text + "\n"
                    logging.debug(f"Page {page_num + 1}: Extracted {len(page_text)} characters")
                if progress_queue and item_id and page_count > 0:
                    progress_percent = int((page_num + 1) / page_count * 100)
                    progress_queue.put(f"Extracting text from PDF for item {item_id}: {progress_percent}%")
                if page_num > 50:
                    logging.info(f"Limiting extraction to first 50 pages")
                    break
            except Exception as e:
                logging.warning(f"Failed to extract page {page_num + 1}: {e}")
                continue
        pdf.close()
        if progress_queue and item_id:
            progress_queue.put(f"Extraction completed for item {item_id}")
        if not text.strip():
            logging.error(f"No text extracted (all {page_count} pages)")
        else:
            logging.info(f"Extracted {len(text)} characters")
        return text
    except Exception as e:
        logging.error(f"Failed to open or process PDF: {e}")
        return ""
def chunk_text_safe(text, chunk_size=CHUNK_SIZE, overlap=CHUNK_OVERLAP, progress_queue=None, item_id=None):
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    start = 0
    total_length = len(text)
    while start < len(text):
        end = min(start + chunk_size, len(text))
        if end < len(text):
            sentence_end = text.rfind('.', start + chunk_size - 200, end)
            if sentence_end > start:
                end = sentence_end + 1
            else:
                word_end = text.rfind(' ', start + chunk_size - 100, end)
                if word_end > start:
                    end = word_end
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        if progress_queue and item_id:
            progress_percent = int(end / total_length * 100)
            progress_queue.put(f"Chunking text for item {item_id}: {progress_percent}%")
        if end >= len(text):
            break
        start = end - overlap
    if progress_queue and item_id:
        progress_queue.put(f"Chunking completed for item {item_id}")
    logging.info(f"Created {len(chunks)} chunks from {len(text)} characters")
    return chunks
def extract_json_from_response(response_text):
    if not response_text:
        logging.warning("Empty response text received")
        return None
    logging.debug(f"Raw AI response: {response_text[:1000]}...")
    try:
        parsed = json.loads(response_text.strip())
        logging.debug(f"Successfully parsed JSON: {list(parsed.keys())}")
        return parsed
    except json.JSONDecodeError:
        pass
    json_pattern = r'```(?:json)?\s*\n?(.*?)\n?```'
    match = re.search(json_pattern, response_text, re.DOTALL)
    if match:
        json_content = match.group(1).strip()
        try:
            parsed = json.loads(json_content)
            logging.debug(f"Extracted JSON from markdown: {list(parsed.keys())}")
            return parsed
        except json.JSONDecodeError:
            json_content = re.sub(r',\s*}', '}', json_content)
            json_content = re.sub(r',\s*]', ']', json_content)
            try:
                parsed = json.loads(json_content)
                logging.debug(f"Fixed and parsed JSON: {list(parsed.keys())}")
                return parsed
            except json.JSONDecodeError:
                logging.warning(f"Failed to parse JSON even after fixes: {json_content[:200]}...")
    json_start = response_text.find('{')
    json_end = response_text.rfind('}')
    if json_start != -1 and json_end != -1 and json_end > json_start:
        potential_json = response_text[json_start:json_end + 1]
        try:
            parsed = json.loads(potential_json)
            logging.debug(f"Found JSON object: {list(parsed.keys())}")
            return parsed
        except json.JSONDecodeError:
            logging.warning(f"Failed to parse extracted JSON: {potential_json[:200]}...")
    logging.error("No valid JSON found in response")
    return None
def get_bitstreams_from_bundles(bundles_url):
    try:
        response = make_api_request_safe(bundles_url)
        bundles_data = response.json()
        bundles = bundles_data.get("_embedded", {}).get("bundles", [])
        logging.info(f"Found {len(bundles)} bundles")
        bitstreams = []
        for bundle in bundles:
            bundle_name = bundle.get("name", "unnamed")
            bitstreams_url = bundle.get("_links", {}).get("bitstreams", {}).get("href")
            logging.info(f"Bundle: {bundle_name}, has bitstreams: {bool(bitstreams_url)}")
            if bitstreams_url:
                try:
                    response = make_api_request_safe(bitstreams_url)
                    bitstreams_data = response.json()
                    bundle_bitstreams = bitstreams_data.get("_embedded", {}).get("bitstreams", [])
                    for bs in bundle_bitstreams:
                        mime_type = bs.get("mimeType", "unknown")
                        name = bs.get("name", "unnamed")
                        logging.info(f" Bundle bitstream: {name} (type: {mime_type})")
                    bitstreams.extend(bundle_bitstreams)
                except Exception as e:
                    logging.warning(f"Failed to get bitstreams from bundle {bundle_name}: {e}")
        pdf_bitstreams = [b for b in bitstreams if
                         b.get("mimeType") == "application/pdf" or
                         b.get("name", "").lower().endswith(".pdf")]
        logging.info(f"Found {len(pdf_bitstreams)} PDF bitstreams via bundles")
        return pdf_bitstreams
    except Exception as e:
        logging.error(f"Failed to fetch bitstreams from bundles: {e}")
        return []
def get_item_details_safe(item, driver=None):
    try:
        indexable_object = item.get("_embedded", {}).get("indexableObject", {})
        item_id = indexable_object.get("uuid")
        if not item_id:
            logging.info(f"Skipping item: No UUID found")
            return None, [], None
     
        url = f"{API_BASE_URL}/core/items/{item_id}"
        response = make_api_request_safe(url)
        item_data = response.json()
        logging.info(f"Fetched item {item_id}: {item_data.get('name', 'No title')}")
        metadata = extract_metadata_from_web(item_id, item_data, driver)
        bitstreams_url = item_data.get("_links", {}).get("bitstreams", {}).get("href")
        bitstreams = []
        if bitstreams_url:
            try:
                response = make_api_request_safe(bitstreams_url)
                bitstreams_data = response.json()
                all_bitstreams = bitstreams_data.get("_embedded", {}).get("bitstreams", [])
                logging.info(f"Item {item_id}: Found {len(all_bitstreams)} total bitstreams via direct link")
                bitstreams = [b for b in all_bitstreams if
                              b.get("mimeType") == "application/pdf" or
                              b.get("name", "").lower().endswith(".pdf")]
                logging.info(f"Found {len(bitstreams)} PDF bitstreams for item {item_id}")
            except Exception as e:
                logging.warning(f"Failed to get bitstreams directly for {item_id}: {e}")
        if len(bitstreams) == 0:
            bundles_url = item_data.get("_links", {}).get("bundles", {}).get("href")
            if bundles_url:
                logging.info(f"No PDFs via direct bitstreams, trying bundles for item {item_id}")
                bitstreams = get_bitstreams_from_bundles(bundles_url)
            else:
                logging.info(f"Item {item_id} has no bundles URL either")
     
        return item_data, bitstreams, metadata
    except Exception as e:
        logging.error(f"Failed to get item details for {item.get('uuid', 'unknown')}: {e}")
        return None, [], None
def save_to_excel(data_list, features, progress_queue=None, extract_ai=False, selected_base_fields=None):
    if not data_list:
        logging.info("No new data to save to Excel")
        return
    def normalize_col_name(col):
        return col.lower().replace(' ', '_').replace('/', '_').replace('-', '_')
    def is_valid_row(row, min_required_non_null):
        non_null_count = sum(1 for val in row if val is not None and val != "" and pd.notna(val))
        return non_null_count >= min_required_non_null
    try:
        # Define base columns and semantic columns
        base_columns = selected_base_fields or ["title", "date", "author_organization", "geography_focus", "type", "source", "url"]
        normalized_base_columns = [normalize_col_name(col) for col in base_columns]
        semantic_columns = [normalize_col_name(feature[0]) for feature in features] if extract_ai else []
        columns = normalized_base_columns + semantic_columns
        columns = list(dict.fromkeys(columns)) # Remove duplicates, preserve order
        # Validate and prepare new data
        new_data = []
        num_cols = len(columns)
        min_required_non_null = len(normalized_base_columns) # Require at least base columns to be non-null
        for row_idx, row in enumerate(data_list):
            row = row[:num_cols] + [None] * (num_cols - len(row)) if len(row) < num_cols else row[:num_cols]
            non_null_count = sum(1 for val in row[:len(normalized_base_columns)] if val is not None and val != "")
            if non_null_count < min_required_non_null:
                logging.warning(f"Skipping malformed new row {row_idx + 1}: {row[:10]}... (only {non_null_count}/{min_required_non_null} non-null base values)")
                continue
            new_row = {}
            for idx, col in enumerate(columns):
                new_row[col] = row[idx]
            new_data.append(new_row)
            logging.debug(f"Validated new row {row_idx + 1}: {list(new_row.values())[:10]}...")
        if not new_data:
            logging.warning("No valid new rows to save after validation")
            return
        # Use openpyxl for memory-efficient append
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(columns) # Write header
        # Append new rows with progress
        appended_count = 0
        total_rows = len(new_data)
        if progress_queue:
            progress_queue.put("Composing Excel: 0%")
        for row_idx, new_row_dict in enumerate(new_data):
            row_values = [new_row_dict.get(col) for col in columns]
            ws.append(row_values)
            appended_count += 1
            if progress_queue and total_rows > 0:
                progress_percent = int((appended_count / total_rows) * 100)
                progress_queue.put(f"Composing Excel: {progress_percent}%")
        # Remove unnecessary columns (if any appeared)
        placeholders = {'or', 'even', 'null', 'always', 'iso'}
        unnecessary_columns = [
            idx + 1 for idx, col in enumerate(columns)
            if (len(str(col)) <= 3 or str(col).isdigit() or str(col) in placeholders)
            and col not in normalized_base_columns # Only check against base columns
        ]
        if unnecessary_columns:
            for col_idx in sorted(unnecessary_columns, reverse=True):
                ws.delete_cols(col_idx)
            logging.info(f"Removed {len(unnecessary_columns)} unnecessary columns")
        # Save locally
        wb.save(EXCEL_FILE)
        logging.info(f"Appended {appended_count} new rows to {EXCEL_FILE}. Total rows: {ws.max_row}")
        if progress_queue:
            progress_queue.put("Excel composition completed")
    except Exception as e:
        logging.error(f"Failed to save to {EXCEL_FILE}: {e}")
        raise
# Streamlit-specific functions
def set_default_prompt():
    return """You are a semantic analysis AI. Extract key information from the provided text.
Return ONLY a valid JSON object with these exact keys:
- key_information (list): Main points or themes extracted from the text
Document text:
\"\"\"
{chunk}
\"\"\"
"""
def query_ai_single_chunk_safe(chunk, item_id, chunk_index, prompt, features, ai_provider, ai_model):
    if not running_event.is_set(): # Check stop signal before acquiring semaphore
        logging.info(f"Stopping AI query for chunk {chunk_index + 1} due to stop signal")
        return None
    if not current_gemini_requests.acquire(timeout=GEMINI_TIMEOUT):
        logging.error(f"Timeout acquiring semaphore for chunk {chunk_index + 1}")
        return None
    try:
        with request_count_lock:
            global active_requests
            active_requests += 1
            logging.info(f"Active {ai_provider} requests: {active_requests}")
        if not running_event.is_set(): # Check again before AI call
            logging.info(f"Stopping AI query for chunk {chunk_index + 1} due to stop signal")
            return None
        for attempt in range(3):
            if not running_event.is_set(): # Check before each attempt
                logging.info(f"Stopping AI query for chunk {chunk_index + 1} attempt {attempt + 1} due to stop signal")
                return None
            try:
                if check_timeout():
                    logging.warning("Timeout reached, stopping chunk processing")
                    return None
                logging.info(f"Querying {ai_provider} for chunk {chunk_index + 1} (attempt {attempt + 1})")
                if ai_provider == "Gemini":
                    response = ai_model.generate_content(prompt.format(chunk=chunk))
                    if not response or not response.text:
                        logging.error(f"Empty response from {ai_provider} for chunk {chunk_index + 1}")
                        continue
                    result = response.text.strip()
                elif ai_provider == "ChatGPT":
                    response = ai_model.chat.completions.create(
                        model="gpt-4o",
                        messages=[{"role": "system", "content": prompt.format(chunk=chunk)}]
                    )
                    if not response or not response.choices[0].message.content:
                        logging.error(f"Empty response from {ai_provider} for chunk {chunk_index + 1}")
                        continue
                    result = response.choices[0].message.content.strip()
                logging.debug(f"Raw {ai_provider} response for chunk {chunk_index + 1}: {result[:500]}...")
                parsed = extract_json_from_response(result)
                if parsed and any(parsed.get(key) for key, _, _ in features):
                    return parsed
                else:
                    logging.error(f"No meaningful content in response for chunk {chunk_index + 1}")
            except Exception as e:
                logging.error(f"{ai_provider} API error for chunk {chunk_index + 1} (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    time.sleep(2 ** attempt)
        logging.error(f"All attempts failed for chunk {chunk_index + 1}")
        return None
    finally:
        with request_count_lock:
            active_requests -= 1
        current_gemini_requests.release()
def query_ai_for_semantic_metadata(pdf_text, item_id, prompt, features, ai_provider, ai_model, progress_queue=None):
    chunks = chunk_text_safe(pdf_text, progress_queue=progress_queue, item_id=item_id)
    logging.info(f"Processing {len(chunks)} chunks for semantic extraction for item {item_id}")
    semantic_metadata = {key: default for key, _, default in features}
    successful_chunks = 0
    for i, chunk in enumerate(chunks):
        if not running_event.is_set() or check_timeout():
            logging.info(f"Stopping chunk {i + 1} processing due to stop signal or timeout")
            break
        logging.info(f"Processing chunk {i + 1}/{len(chunks)} for item {item_id}")
        parsed_result = query_ai_single_chunk_safe(chunk, item_id, i, prompt, features, ai_provider, ai_model)
        if parsed_result:
            successful_chunks += 1
            for key, type_desc, default in features:
                value = parsed_result.get(key, default)
                if value is None or value == default:
                    continue
                if "list" in type_desc:
                    current = semantic_metadata[key]
                    if isinstance(value, list):
                        current.extend([v for v in value if v])
                    else:
                        current.append(value)
                    # Deduplicate with handling for unhashable types
                    try:
                        semantic_metadata[key] = list(dict.fromkeys(current)) # Preserve order, avoid set()
                    except TypeError as e:
                        logging.warning(f"Unhashable type in {key} for item {item_id}: {e}, value: {current}")
                        # Convert unhashable items to strings
                        unique_items = []
                        seen = set()
                        for item in current:
                            try:
                                item_str = json.dumps(item, sort_keys=True) if isinstance(item, (dict, list)) else str(item)
                            except TypeError:
                                item_str = str(item)
                            if item_str not in seen:
                                seen.add(item_str)
                                unique_items.append(item)
                        semantic_metadata[key] = unique_items
                        logging.info(f"Deduplicated {key} using string representation: {len(unique_items)} items")
                elif "boolean" in type_desc:
                    if value:
                        semantic_metadata[key] = True
                elif "integer" in type_desc or "float" in type_desc:
                    try:
                        semantic_metadata[key] = max(semantic_metadata[key], float(value))
                    except (ValueError, TypeError):
                        logging.warning(f"Invalid number for {key}: {value}")
                else:
                    semantic_metadata[key] = value
        if progress_queue:
            progress_queue.put(f"Analyzing chunk {i + 1}/{len(chunks)} for item {item_id}")
        time.sleep(0.1)
    logging.info(f"Processed {successful_chunks}/{len(chunks)} chunks for semantic extraction of item {item_id}")
    logging.info(f"Final aggregated metadata: {json.dumps(semantic_metadata, indent=2)[:1000]}...")
    if progress_queue:
        progress_queue.put(f"Analysis completed for item {item_id}")
    return semantic_metadata
def download_pdf_safe(pdf_url, filename, page_num, item_id, prompt, features, ai_provider, ai_model, progress_queue):
    if not running_event.is_set():
        logging.info(f"Stopping PDF download for {pdf_url} due to stop signal")
        return None, None
    semantic_metadata = None
    tmp_path = None
    try:
        response = make_api_request_safe(pdf_url, stream=True, timeout=60)
        total_size = int(response.headers.get('content-length', 0))
        downloaded_size = 0
        tmp_path = tempfile.mktemp(suffix='.pdf')
        with open(tmp_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if not running_event.is_set():
                    logging.info(f"Stopping PDF download for {pdf_url} during streaming due to stop signal")
                    response.close()
                    if tmp_path and os.path.exists(tmp_path):
                        os.unlink(tmp_path)
                    return None, None
                if chunk:
                    f.write(chunk)
                    downloaded_size += len(chunk)
                    if total_size > 0:
                        progress = downloaded_size / total_size
                        progress_queue.put(f"Downloading PDF for item {item_id}: {int(progress * 100)}%")
                    else:
                        progress_queue.put(f"Downloading PDF for item {item_id}: {downloaded_size} bytes")
        progress_queue.put(f"Download completed for item {item_id}")
        if not running_event.is_set():
            logging.info(f"Stopping PDF text extraction for {pdf_url} due to stop signal")
            return None, None
        pdf_text = extract_pdf_text_safe(tmp_path, progress_queue=progress_queue, item_id=item_id)
        progress_queue.put(f"Extraction completed for item {item_id}")
        if not running_event.is_set():
            logging.info(f"Stopping semantic processing for {pdf_url} due to stop signal")
            return None, None
        semantic_metadata = query_ai_for_semantic_metadata(pdf_text, item_id, prompt, features, ai_provider, ai_model, progress_queue=progress_queue)
    except Exception as e:
        logging.error(f"Failed to process PDF {pdf_url}: {e}")
        progress_queue.put(f"Error processing PDF for item {item_id}: {str(e)}")
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
        return None, semantic_metadata
    

def process_single_item_safe(item, page_num, driver, prompt, features, ai_provider, ai_model, progress_queue, extract_ai, selected_base_fields):
    if not running_event.is_set():
        logging.info("Skipping item due to stop signal")
        progress_queue.put("Skipping item due to stop signal")
        return None
    try:
        item_data, bitstreams, metadata = get_item_details_safe(item, driver)
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
            processed_items.add(item_id)
        except TypeError as e:
            logging.error(f"Failed to add item_id to processed_items: {item_id}, error: {e}")
            progress_queue.put(f"Error processing item {item_id}: {str(e)}")
            return None
        progress_queue.put(f"Extracted metadata for item {item_id}")
        logging.debug(f"Item data for {item_id}: {json.dumps(item_data, indent=2)[:1000]}")
        logging.debug(f"Bitstreams for {item_id}: {json.dumps(bitstreams, indent=2)[:1000]}")
        semantic_metadata = {key: default for key, _, default in features} if extract_ai else {}
        if extract_ai:
            for bitstream in bitstreams[:1]:
                if not running_event.is_set():
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
                        _, extracted_metadata = download_pdf_safe(pdf_url, filename, page_num, item_id, prompt, features, ai_provider, ai_model, progress_queue)
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
        # No need for fixed fallback; metadata now has all keys
        base_metadata = [metadata.get(FIELD_MAPPING.get(field, field), "Unknown") for field in selected_base_fields]
        semantic_values = []
        if extract_ai:
            for key, type_desc, default in features:
                value = semantic_metadata.get(key, default)
                if "list" in type_desc:
                    value = "; ".join(str(v) for v in value) if value else "Not detected"
                elif value is None or value == default:
                    value = "Unknown" if "string" in type_desc else value
                semantic_values.append(value)
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
    
def reset_progress_state():
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
    st.session_state.progress_queue = Queue()
    processed_items.clear()  # Clear processed items set
    logging.info("Progress state reset")

    
# Streamlit App
def main():
    global gemini_model, GEMINI_AVAILABLE, CHUNK_SIZE, MAX_PARALLEL_GEMINI_REQUESTS, REQUEST_DELAY, BASE_SAVE_DIR, EXCEL_FILE, current_gemini_requests, chatgpt_client, CHATGPT_AVAILABLE
    setup_logging()
 
    # UI Layout
    col1, col2 = st.columns([1, 5])
    with col1:
        st.image('CGIAR-logo.png', width=100)
    with col2:
        st.title("CGSpace Data Extractor & AI Analyzer")
 
    # Initialize Session State
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
        st.session_state.prompt = set_default_prompt()
        st.session_state.driver = None
        st.session_state.selenium_status = "Not Initialized"
        st.session_state.features = parse_prompt_for_features(st.session_state.prompt)
        st.session_state.ai_provider = "Gemini"
        st.session_state.progress_queue = Queue()
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
 
    # Get API Keys
    default_gemini_api_key = os.getenv("GEMINI_API_KEY", "")
    default_chatgpt_api_key = os.getenv("OPENAI_API_KEY", "")
    try:
        default_gemini_api_key = st.secrets["GEMINI_API_KEY"]
        default_chatgpt_api_key = st.secrets["OPENAI_API_KEY"]
    except (KeyError, FileNotFoundError):
        pass
 
    # Sidebar Settings
    with st.sidebar:
        st.header("AI Analysis Prompt")
        prompt_input = st.text_area("Prompt", value=st.session_state.prompt, height=300)
        st.subheader("Parsed Features")
        if prompt_input != st.session_state.prompt:
            st.session_state.features = parse_prompt_for_features(prompt_input)
        st.write("Features to be extracted:")
        for key, type_desc, default in st.session_state.features:
            st.write(f"- {key} ({type_desc}, default: {default})")

        st.header("Settings")
        st.session_state.ai_provider = st.radio("AI Provider", ["Gemini", "ChatGPT 4.1"])
        api_key = st.text_input(
            f"{st.session_state.ai_provider} API Key",
            value=default_gemini_api_key if st.session_state.ai_provider == "Gemini" else default_chatgpt_api_key,
            type="password"
        )
        chunk_size = st.number_input("Chunk Size", value=CHUNK_SIZE, min_value=500)
        max_requests = st.number_input("Max Parallel Requests", value=MAX_PARALLEL_GEMINI_REQUESTS, min_value=1)
        request_delay = st.number_input("Request Delay (seconds)", value=REQUEST_DELAY, min_value=0.1)
        excel_file = st.text_input("Excel Output File", value=EXCEL_FILE)
     
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Reset Prompt"):
                st.session_state.prompt = set_default_prompt()
                st.session_state.features = parse_prompt_for_features(st.session_state.prompt)
                st.rerun()
        with col2:
            if st.button("Apply Settings"):
                try:
                    CHUNK_SIZE = int(chunk_size)
                    MAX_PARALLEL_GEMINI_REQUESTS = int(max_requests)
                    REQUEST_DELAY = float(request_delay)
                    EXCEL_FILE = excel_file
                    st.session_state.prompt = prompt_input
                    st.session_state.features = parse_prompt_for_features(prompt_input)
                    current_gemini_requests = threading.Semaphore(MAX_PARALLEL_GEMINI_REQUESTS)
                    st.success("Settings applied successfully!")
                    logging.info(f"Settings updated successfully. Features: {[f[0] for f in st.session_state.features]}")
                except ValueError as e:
                    st.error(f"Invalid setting value: {e}")
        
        st.header("Extraction Options")
        
        st.session_state.selected_base_fields = st.multiselect(
            "Select Base Metadata Fields",
            options=["title", "date", "author_organization", "geography_focus", "type", "source", "url", "abstract", "language", "doi", "place", "coverage.region", "coverage.country", "contributor.affiliation", "creator.id", "authorship.types", "journal", "volume", "issue", "isbn", "subject.impactArea", "subject.actionArea", "contributor.donor", "contributor.project", "contributor.initiative", "file.mimetype", "file.filename", "identifier.citation"],
            default=st.session_state.selected_base_fields
        )
        st.session_state.extract_ai = st.toggle("Extract Metadata + AI Analyzed Data", value=st.session_state.extract_ai, help="Toggle off to extract only metadata")
 
    # Main Interface
    st.subheader("Search Configuration")
    query = st.text_input("Search Query", value="Artificial Intelligence")
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
    iwmi_filter = st.toggle("Filter by IWMI Author Affiliation", value=False)

    # Reset progress on toggle change
    if 'previous_extract_ai' not in st.session_state:
        st.session_state.previous_extract_ai = st.session_state.extract_ai
    if st.session_state.extract_ai != st.session_state.previous_extract_ai:
        st.session_state.previous_extract_ai = st.session_state.extract_ai
        st.session_state.overall_progress_value = 0
        st.session_state.overall_progress_text = "Overall Progress"
        st.session_state.pdf_progress_value = 0
        st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
        st.session_state.excel_progress_value = 0
        st.session_state.excel_progress_text = "Excel Composing Progress"
        st.session_state.total_processed = 0
        st.session_state.pdfs_downloaded = 0
        st.session_state.ai_analyses = 0
        st.session_state.results = []
        st.session_state.progress_queue = Queue()  # Clear queue
        st.session_state.query_validated = False
 
    # Normalize year inputs
    min_year = None if min_year_input == 2000 else str(min_year_input)
    max_year = None if max_year_input == 2025 else str(max_year_input)
    if min_year and max_year and int(min_year) > int(max_year):
        st.warning("Min year should not exceed max year; results may be empty.")
 
   
    def on_search():
        if not query:
            st.session_state.search_success = "Please enter a search query."
            return
        try:
            total_items = get_total_count(query, min_year, max_year, iwmi_filter)
            st.session_state.total_samples = total_items
            st.session_state.total_items = total_items
            st.session_state.total_pages = math.ceil(total_items / 10) if total_items > 0 else 0
            st.session_state.query_validated = True
            st.session_state.display_start = 1
            st.session_state.display_end = min(10, total_items)
            # Reset progress states on new search
            st.session_state.overall_progress_value = 0
            st.session_state.overall_progress_text = "Overall Progress"
            st.session_state.pdf_progress_value = 0
            st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
            st.session_state.excel_progress_value = 0
            st.session_state.excel_progress_text = "Excel Composing Progress"
            st.session_state.total_processed = 0
            st.session_state.pdfs_downloaded = 0
            st.session_state.ai_analyses = 0
            st.session_state.results = []
            st.session_state.progress_queue = Queue()  # Clear queue for new process
            filter_info = "with IWMI filter" if iwmi_filter else "without IWMI filter"
            logging.info(f"API response totalElements: {total_items} for query '{query}' with filters (min_year={min_year}, max_year={max_year}, iwmi_filter={iwmi_filter})")
            st.session_state.search_success = f"Query validated: {total_items} samples found across {st.session_state.total_pages} pages {filter_info}. You can adjust start/end pages as needed."
        except Exception as e:
            st.session_state.search_success = f"Error validating query: {str(e)}"
            logging.error(f"Search query failed: {str(e)}")
 
    # Search Button
    st.button("Search", on_click=on_search)
 
    # Display Success/Error Message
    if st.session_state.search_success:
        if "Error" in st.session_state.search_success or "Please enter a search query" in st.session_state.search_success:
            st.error(st.session_state.search_success)
            st.session_state.query_validated = False
        else:
            st.success(st.session_state.search_success)
        st.session_state.search_success = None # Clear after display
 
    # Display Total Pages and Samples (only after search)
    totals_text = st.empty()
    if st.session_state.query_validated:
        totals_text.text(f"Total pages: {st.session_state.total_pages} | Total samples: {st.session_state.total_samples}")
    # Display Search Result Range (only after search)
    search_result_text = st.empty()
    if st.session_state.query_validated:
        search_result_text.text(f"Now showing {st.session_state.display_start} - {st.session_state.display_end} of {st.session_state.total_items}")
 
    # Control Buttons
    col3, col4 = st.columns(2)
    with col3:
        start_button = st.button("Start Processing", disabled=st.session_state.is_running or not st.session_state.query_validated)
    with col4:
        stop_button = st.button("Stop Processing", disabled=not st.session_state.is_running)
 
    # Download Excel (always available if file exists)
    if not st.session_state.is_running and os.path.exists(EXCEL_FILE):
        st.subheader("Download Results")
        with open(EXCEL_FILE, "rb") as f:
            st.download_button(
                label="Download Excel File",
                data=f,
                file_name=EXCEL_FILE,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_file"
            )
    elif not st.session_state.is_running and st.session_state.total_processed > 0:
        st.info("Processing complete, but Excel file not found.")


    def run_scraper(query, start_page, end_page, api_key, driver, results, total_processed, pdfs_downloaded, ai_analyses, prompt, features, ai_provider, total_pages, total_samples, min_year, max_year, iwmi_filter, progress_queue, extract_ai, selected_base_fields):
        global processing_start_time, gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE
        if extract_ai:
            gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE = initialize_ai(ai_provider, api_key)
            if (ai_provider == "Gemini" and not GEMINI_AVAILABLE) or (ai_provider == "ChatGPT" and not CHATGPT_AVAILABLE):
                logging.error("Cannot start processing: AI API not initialized")
                progress_queue.put("Error: AI API not initialized")
                return results, total_processed, pdfs_downloaded, ai_analyses, []
        else:
            gemini_model, GEMINI_AVAILABLE, chatgpt_client, CHATGPT_AVAILABLE = None, False, None, False
        processing_start_time = time.time()
        if os.path.exists(EXCEL_FILE):
            os.remove(EXCEL_FILE)
            logging.info(f"Deleted existing {EXCEL_FILE} to start fresh.")
        if not driver:
            driver = setup_selenium_driver()
            if driver:
                logging.info("Selenium WebDriver initialized successfully")
            else:
                logging.error("Failed to initialize Selenium WebDriver")
                progress_queue.put("Error: Failed to initialize Selenium WebDriver")
        progress_updates = []
        processed_lock = threading.Lock()
        try:
            total_items = get_total_count(query, min_year, max_year, iwmi_filter)
            expected_samples = (end_page - start_page + 1) * 10
            total_samples = min(total_items - (start_page - 1) * 10, expected_samples)
            progress_queue.put(f"Total samples: {total_samples}")
            if total_samples <= 0:
                logging.info("No items in selected range. Stopping.")
                progress_updates.append("No items in selected range. Stopping.")
                return results, total_processed, pdfs_downloaded, ai_analyses, progress_updates
            total_pages = math.ceil(total_items / 10)
            for page in range(start_page, end_page + 1):
                if not running_event.is_set() or check_timeout():
                    logging.info(f"Stopping page loop: running_event={running_event.is_set()}, timeout={check_timeout()}")
                    progress_updates.append("Stopped during page processing")
                    break
                logging.info(f"Processing page {page}...")
                page_start_time = time.time()
                items, page_total = search_items(query, page, min_year, max_year, iwmi_filter, size=10)
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
                    if not running_event.is_set() or check_timeout():
                        logging.info("Stopped during item processing")
                        progress_updates.append("Stopped during item processing")
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
                    result = process_single_item_safe(item, page, driver, prompt, features, ai_provider, ai_model, progress_queue, extract_ai, selected_base_fields)
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
                    time.sleep(0.1)
                if results:
                    save_to_excel(results, features, progress_queue=progress_queue, extract_ai=extract_ai, selected_base_fields=selected_base_fields)
                    results.clear()
                page_time = time.time() - page_start_time
                logging.info(f"Page {page} completed in {page_time:.2f} seconds. Processed {total_processed} items.")
                progress_updates.append(f"Page {page} completed")
                time.sleep(1)
            total_time = time.time() - processing_start_time
            logging.info(f"Processing completed! Total items: {total_processed}, Time: {total_time:.2f}s")
            progress_updates.append(f"Processing completed! Total items: {total_processed}, Time: {total_time:.2f}s")
            return results, total_processed, pdfs_downloaded, ai_analyses, progress_updates
        except Exception as e:
            logging.error(f"Unexpected error in run_scraper: {e}")
            progress_updates.append(f"Error during processing: {str(e)}")
            return results, total_processed, pdfs_downloaded, ai_analyses, progress_updates
        finally:
            if driver:
                try:
                    driver.quit()
                    logging.info("Selenium driver closed")
                except Exception as e:
                    logging.warning(f"Error closing driver: {e}")
            if results:
                save_to_excel(results, features, progress_queue=progress_queue, extract_ai=extract_ai, selected_base_fields=selected_base_fields)
                results.clear()
            return results, total_processed, pdfs_downloaded, ai_analyses, progress_updates
 
    # Button Actions

    if start_button:
        if not query or end_page < start_page:
            st.error("Invalid input. Ensure query is not empty and end page >= start_page.")
        elif st.session_state.extract_ai and not api_key:
            st.error("Please enter the API key before starting processing.")
        elif st.session_state.extract_ai and not st.session_state.features:
            st.error("No valid features found in the prompt. Please update the prompt or reset to default.")
        elif not st.session_state.query_validated:
            st.error("Please validate the query using the Search button first.")
        elif not st.session_state.selected_base_fields:
            st.error("Please select at least one base metadata field.")
        else:
            total_to_process = max(0, min(st.session_state.total_samples - (start_page - 1) * 10, (end_page - start_page + 1) * 10))
            st.session_state.is_running = True
            running_event.set()
            st.session_state.processing_start_time = time.time()
            # Reset progress and queue before starting
            st.session_state.progress_queue = Queue()
            st.session_state.overall_progress_value = 0
            st.session_state.overall_progress_text = "Overall Progress"
            st.session_state.pdf_progress_value = 0
            st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
            st.session_state.excel_progress_value = 0
            st.session_state.excel_progress_text = "Excel Composing Progress"
            st.session_state.total_processed = 0
            st.session_state.pdfs_downloaded = 0
            st.session_state.ai_analyses = 0
            st.session_state.results = []
            extract_ai = st.session_state.extract_ai
            selected_base_fields = st.session_state.selected_base_fields.copy()
            def update_session_state(query, start_page, end_page, api_key, driver, results, total_processed, pdfs_downloaded, ai_analyses, prompt, features, ai_provider, progress_queue, total_pages, total_to_process, min_year, max_year, iwmi_filter, extract_ai, selected_base_fields):
                results, total_processed, pdfs_downloaded, ai_analyses, progress_updates = run_scraper(
                    query, start_page, end_page, api_key, driver, results, total_processed, pdfs_downloaded, ai_analyses, prompt, features, ai_provider, total_pages, total_to_process, min_year, max_year, iwmi_filter, progress_queue, extract_ai, selected_base_fields
                )
                for update in progress_updates:
                    progress_queue.put(update)
                progress_queue.put({'type': 'final', 'results': results, 'total_processed': total_processed, 'pdfs_downloaded': pdfs_downloaded, 'ai_analyses': ai_analyses})
                progress_queue.put(None)
            threading.Thread(
                target=update_session_state,
                args=(query, start_page, end_page, api_key, st.session_state.driver, st.session_state.results, st.session_state.total_processed, st.session_state.pdfs_downloaded, st.session_state.ai_analyses, st.session_state.prompt, st.session_state.features, st.session_state.ai_provider, st.session_state.progress_queue, st.session_state.total_pages, total_to_process, min_year, max_year, iwmi_filter, extract_ai, selected_base_fields),
                daemon=True
            ).start()
            st.session_state.last_update_time = time.time()
            st.rerun()

        if stop_button:
            st.session_state.is_running = False
            running_event.clear()
            logging.info("Processing stopped by user")
            st.session_state.progress_queue.put("Processing stopped by user")
            # Reset all session states to reinitialize
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
            st.session_state.prompt = set_default_prompt()
            st.session_state.features = parse_prompt_for_features(st.session_state.prompt)
            st.session_state.progress_queue = Queue()
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
            # Close Selenium driver if open to free resources
            if st.session_state.driver:
                try:
                    st.session_state.driver.quit()
                    logging.info("Selenium driver closed on stop")
                except Exception as e:
                    logging.warning(f"Error closing driver on stop: {e}")
                st.session_state.driver = None
                st.session_state.selenium_status = "Not Initialized"
            st.rerun()
 
    # Progress and Stats (persistent status block)
    status_placeholder = st.empty()
    if st.session_state.is_running:
        with status_placeholder.status("Processing Status", expanded=True) as status:
            overall_progress = st.progress(st.session_state.overall_progress_value, text=st.session_state.overall_progress_text)
            pdf_progress = st.progress(st.session_state.pdf_progress_value, text=st.session_state.pdf_progress_text)
            excel_progress = st.progress(st.session_state.excel_progress_value, text=st.session_state.excel_progress_text)
          
            time_text = st.empty()
            stats_text = st.empty()
           
            # Quick Actions
            st.subheader("Quick Actions")
            if st.button("Download Excel", key="download_excel"):
                if os.path.exists(EXCEL_FILE):
                    with open(EXCEL_FILE, "rb") as f:
                        st.download_button(
                            label="Download Excel File",
                            data=f,
                            file_name=EXCEL_FILE,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            key="download_excel_file"
                        )
                else:
                    st.info("Excel file doesn't exist yet. Start processing to create it.")
            # Progress Update Loop
            processed_any = False
            while True:
                try:
                    update = st.session_state.progress_queue.get_nowait()
                    logging.info(f"Queue update: {update}")
                    processed_any = True
                    if update is None:
                        st.session_state.is_running = False
                        st.session_state.overall_progress_value = 1.0
                        st.session_state.overall_progress_text = "Overall Progress: Completed"
                        st.session_state.pdf_progress_value = 0
                        st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
                        st.session_state.excel_progress_value = 0
                        st.session_state.excel_progress_text = "Excel Composing Progress"
                        status.update(label="Processing completed!", state="complete", expanded=False)
                        break
                    elif isinstance(update, dict) and update.get('type') == 'final':
                        st.session_state.results = update['results']
                        st.session_state.total_processed = update['total_processed']
                        st.session_state.pdfs_downloaded = update['pdfs_downloaded']
                        st.session_state.ai_analyses = update['ai_analyses']
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
                        if "Total samples" in update:
                            total_samples = int(update.split(": ")[1])
                            st.session_state.total_samples = total_samples
                            st.session_state.total_items = total_samples
                            st.session_state.total_pages = math.ceil(total_samples / 10) if total_samples > 0 else 0
                            totals_text.text(f"Total pages: {st.session_state.total_pages} | Total samples: {st.session_state.total_samples}")
                            st.session_state.display_start = 1
                            st.session_state.display_end = min(10, total_samples)
                            search_result_text.text(f"Now showing {st.session_state.display_start} - {st.session_state.display_end} of {st.session_state.total_items}")
                        elif "Display range" in update:
                            range_text = update.split(": ")[1]
                            parts = re.findall(r'\d+', range_text)
                            if len(parts) == 3:
                                start, end, total = map(int, parts)
                                st.session_state.display_start = start
                                st.session_state.display_end = end
                                st.session_state.total_items = total
                                search_result_text.text(f"Now showing {start} - {end} of {total}")
                            else:
                                logging.warning(f"Invalid display range format: {range_text}")
                        elif "Overall progress" in update:
                            percent = int(update.split(": ")[1].split("%")[0])
                            st.session_state.overall_progress_value = min(percent / 100.0, 1.0)
                            st.session_state.overall_progress_text = f"Overall Progress: {percent}% ({st.session_state.total_processed}/{st.session_state.total_samples} items)"
                        elif "Processing item" in update:
                            current_item = int(update.split("item ")[1].split(" ")[0])
                            total_items = int(update.split("of ")[1].split(" ")[0])
                            st.session_state.current_item = current_item
                            st.session_state.total_items_on_page = total_items
                            st.session_state.pdf_progress_value = 0
                            st.session_state.pdf_progress_text = f"PDF Data Extraction Progress: Item {current_item}/{total_items}"
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
                        elif "Composing Excel" in update:
                            if "%" in update:
                                percent = int(update.split(": ")[1].split("%")[0])
                                st.session_state.excel_progress_value = min(percent / 100.0, 1.0)
                                st.session_state.excel_progress_text = f"Excel Composing: {percent}%"
                            else:
                                st.session_state.excel_progress_value = 0
                                st.session_state.excel_progress_text = "Excel Composing Progress"
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
                        elif "Stopped" in update:
                            st.session_state.is_running = False
                            st.session_state.overall_progress_value = 1.0
                            st.session_state.overall_progress_text = "Overall Progress: Stopped"
                            st.session_state.pdf_progress_value = 0
                            st.session_state.pdf_progress_text = "PDF Data Extraction Progress"
                            st.session_state.excel_progress_value = 0
                            st.session_state.excel_progress_text = "Excel Composing Progress"
                            status.update(label="Processing stopped!", state="error", expanded=False)
                            break
                        elif "Error" in update:
                            logging.warning(f"Error message received: {update}")
                            st.session_state.pdf_progress_value = 0
                            st.session_state.pdf_progress_text = f"PDF Data Extraction: Error - {update}"
                        elif "Item completed" in update:
                            parts = update.split(": ")[1].split("/")
                            st.session_state.total_processed = int(parts[0])
                            if st.session_state.extract_ai:
                                st.session_state.pdfs_downloaded += 1
                                st.session_state.ai_analyses += 1
                except Empty:
                    logging.debug(f"Progress queue empty, size: {st.session_state.progress_queue.qsize()}")
                    break
            # Always update stats and time
            elapsed = time.time() - st.session_state.processing_start_time
            time_text.text(f"Elapsed Time: {timedelta(seconds=elapsed)}")
            # Auto-refresh for next updates
            st_autorefresh(interval=1000, key="progress_refresh")
if __name__ == "__main__":
    main()