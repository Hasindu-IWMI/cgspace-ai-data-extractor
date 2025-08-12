import requests
import logging
from config import Config
from metadata_extractor import MetadataExtractor
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from utils import interruptable_sleep
import json


class APIClient:
    def __init__(self, config):
        self.config = config

    def setup_selenium_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()), options=chrome_options)
            # service = Service(ChromeDriverManager().install())
            # driver = webdriver.Chrome(service=service)
            return driver
        except Exception as e:
            logging.error(f"Failed to setup Chrome driver: {e}")
            logging.info("Please install ChromeDriver or use the API-only version")
            return None

    def make_api_request_safe(self, url, params=None, headers=None, retries=3, timeout=10, stream=False):
        if headers is None:
            headers = {"User-Agent": "CGIAR 360 integration bot; h.ramanayake@cgiar.org"}
        if self.config.API_TOKEN:
            headers["Authorization"] = f"Bearer {self.config.API_TOKEN}"
        interruptable_sleep(self.config.REQUEST_DELAY, self.config.running_event)
        for attempt in range(retries):
            try:
                logging.info(f"Making request to {url} (attempt {attempt + 1})")
                response = requests.get(url, params=params, headers=headers, stream=stream, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.exceptions.Timeout:
                logging.warning(f"Timeout for {url} (attempt {attempt + 1})")
                if attempt < retries - 1:
                    interruptable_sleep(2 ** attempt, self.config.running_event)
                    continue
                raise
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    wait_time = 30 * (2 ** attempt)
                    logging.info(f"Rate limited. Waiting {wait_time} seconds...")
                    if not interruptable_sleep(wait_time, self.config.running_event):
                        logging.info(f"Request for {url} stopped during rate limit wait")
                        raise requests.exceptions.RequestException("Process stopped by user")
                    continue
                logging.error(f"HTTP Error for {url}: {e}")
                raise
            except requests.RequestException as e:
                logging.error(f"Request failed for {url}: {e}")
                if attempt < retries - 1:
                    interruptable_sleep(2 ** attempt)
                    continue
                raise
        raise requests.exceptions.RequestException(f"Max retries reached for {url}")

    def search_items(self, query, page, min_year=None, max_year=None, selected_affiliations=[], selected_regions=[], selected_countries=[], size=10):
        url = f"{self.config.API_BASE_URL}/discover/search/objects"
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
        # Add affiliation filters
        if selected_affiliations:
            params["f.affiliation"] = [f"{aff},equals" for aff in selected_affiliations]
        # Add region filters
        if selected_regions:
            params["f.region"] = [f"{r},equals" for r in selected_regions]
        # Add country filters
        if selected_countries:
            params["f.country"] = [f"{c},equals" for c in selected_countries]
        try:
            logging.info(f"Sending API request with params: {params}")
            response = self.make_api_request_safe(url, params=params)
            data = response.json()
            logging.debug(f"Full API response: {json.dumps(data, indent=2)}") # Log full response
            items = data.get("_embedded", {}).get("searchResult", {}).get("_embedded", {}).get("objects", [])
            total_items = data.get("_embedded", {}).get("searchResult", {}).get("page", {}).get("totalElements", len(items))
            if not total_items:
                logging.warning(f"No totalElements found in response for page {page}: {json.dumps(data)}")
            logging.info(f"Page {page}: Found {len(items)} items (Total: {total_items}) with filters min_year={min_year}, max_year={max_year}, affiliations={selected_affiliations}, regions={selected_regions}, countries={selected_countries}")
            return items, total_items
        except requests.exceptions.HTTPError as e:
            logging.error(f"HTTP Error for {url}: {e.response.status_code} - {e.response.text}")
            return [], 0
        except Exception as e:
            logging.error(f"Failed to fetch search results for page {page}: {e}")
            return [], 0

    def get_total_count(self, query, min_year=None, max_year=None, selected_affiliations=[], selected_regions=[], selected_countries=[]):
        _, total_items = self.search_items(query, 1, min_year, max_year, selected_affiliations, selected_regions, selected_countries, size=1) # Use size=1 instead of 0
        logging.info(f"Total count for query '{query}' with filters (min_year={min_year}, max_year={max_year}, affiliations={selected_affiliations}, regions={selected_regions}, countries={selected_countries}): {total_items}")
        return total_items

    def get_bitstreams_from_bundles(self, bundles_url):
        try:
            response = self.make_api_request_safe(bundles_url)
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
                        response = self.make_api_request_safe(bitstreams_url)
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

    def get_item_details_safe(self, item, driver=None):
        try:
            indexable_object = item.get("_embedded", {}).get("indexableObject", {})
            item_id = indexable_object.get("uuid")
            if not item_id:
                logging.info(f"Skipping item: No UUID found")
                return None, [], None
     
            url = f"{self.config.API_BASE_URL}/core/items/{item_id}"
            response = self.make_api_request_safe(url)
            item_data = response.json()
            logging.info(f"Fetched item {item_id}: {item_data.get('name', 'No title')}")
            metadata = MetadataExtractor().extract_metadata_from_web(item_id, item_data, driver)  # Assuming MetadataExtractor class
            bitstreams_url = item_data.get("_links", {}).get("bitstreams", {}).get("href")
            bitstreams = []
            if bitstreams_url:
                try:
                    response = self.make_api_request_safe(bitstreams_url)
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
                    bitstreams = self.get_bitstreams_from_bundles(bundles_url)
                else:
                    logging.info(f"Item {item_id} has no bundles URL either")
     
            # If still no bitstreams and driver available, try scraping PDF link from web page
            if len(bitstreams) == 0 and driver:
                try:
                    web_url = f"https://cgspace.cgiar.org/items/{item_id}"
                    driver.get(web_url)
                    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    # Find potential PDF download links (adjust XPath/CSS as per page structure)
                    pdf_links = soup.find_all('a', href=lambda href: href and '.pdf' in href.lower())
                    if pdf_links:
                        pdf_href = pdf_links[0]['href']
                        if not pdf_href.startswith('http'):
                            pdf_href = 'https://cgspace.cgiar.org' + pdf_href
                        bitstreams.append({
                            "_links": {"content": {"href": pdf_href}},
                            "name": "scraped.pdf",
                            "mimeType": "application/pdf"
                        })
                        logging.info(f"Scraped PDF URL from web page: {pdf_href} for item {item_id}")
                    else:
                        logging.info(f"No PDF links found on web page for {item_id}")
                except Exception as e:
                    logging.warning(f"Failed to scrape PDF link from web for {item_id}: {e}")
     
            return item_data, bitstreams, metadata
        except Exception as e:
            logging.error(f"Failed to get item details for {item.get('uuid', 'unknown')}: {e}")
            return None, [], None