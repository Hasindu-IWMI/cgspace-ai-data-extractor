import json
import logging
import re
from bs4 import BeautifulSoup
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import fitz  # PyMuPDF
from selenium.webdriver.common.by import By

class MetadataExtractor:
    def extract_json_from_response(self, response_text):
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

    def extract_metadata_from_web(self, item_id, item_data, driver):
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
                    metadata[key] = value # Use full key as-is
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