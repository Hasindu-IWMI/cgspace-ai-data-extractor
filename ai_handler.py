from google.generativeai import GenerativeModel, configure
from openai import AzureOpenAI
import logging
import re
import time
import json
from excel_writer import chunk_text_safe, interruptable_sleep
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
logging.getLogger('tensorflow').setLevel(logging.ERROR) 

class AIHandler:
    def __init__(self, config, ai_provider, api_key):
        self.config = config
        self.ai_provider = ai_provider
        self.api_key = api_key
        self.model, self.available, self.client, self.client_available = self.initialize_ai()

    def check_timeout(self):
        if self.config.processing_start_time and time.time() - self.config.processing_start_time > self.config.MAX_TOTAL_PROCESSING_TIME:
            logging.error("Maximum processing time exceeded. Stopping.")
            return True
        return False

    def initialize_ai(self):
        if self.ai_provider == "Gemini":
            try:
                configure(api_key=self.api_key)
                gemini_model = GenerativeModel("gemini-1.5-pro")
                self.config.gemini_model = gemini_model
                self.config.GEMINI_AVAILABLE = True
                return gemini_model, True, None, False
            except Exception as e:
                logging.error(f"Failed to configure Gemini API: {e}")
                return None, False, None, False
        elif self.ai_provider == "ChatGPT":
            try:
                client = AzureOpenAI(
                    azure_endpoint="https://iwmi-gplli-openai.openai.azure.com/",
                    api_key=self.api_key,
                    api_version="2024-12-01-preview" 
                )
                self.config.chatgpt_client = client
                self.config.CHATGPT_AVAILABLE = True
                return None, False, client, True
            except Exception as e:
                logging.error(f"Failed to configure Azure OpenAI (ChatGPT) API: {e}")
                return None, False, None, False

    def parse_prompt_for_features(self, prompt):
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

    def query_ai_single_chunk_safe(self, chunk, item_id, chunk_index, prompt, features):
        if not self.config.running_event.is_set():
            logging.info(f"Stopping AI query for chunk {chunk_index + 1} due to stop signal")
            return None
        if not self.config.current_gemini_requests.acquire(timeout=self.config.GEMINI_TIMEOUT):
            logging.error(f"Timeout acquiring semaphore for chunk {chunk_index + 1}")
            return None
        try:
            with self.config.request_count_lock:
                self.config.active_requests.value += 1
                logging.info(f"Active {self.ai_provider} requests: {self.config.active_requests.value}")
            if not self.config.running_event.is_set():
                logging.info(f"Stopping AI query for chunk {chunk_index + 1} due to stop signal")
                return None
            for attempt in range(3):
                if not self.config.running_event.is_set():
                    logging.info(f"Stopping AI query for chunk {chunk_index + 1} attempt {attempt + 1} due to stop signal")
                    return None
                try:
                    if self.config.processing_start_time and time.time() - self.config.processing_start_time > self.config.MAX_TOTAL_PROCESSING_TIME:
                        logging.warning("Timeout reached, stopping chunk processing")
                        return None
                    logging.info(f"Querying {self.ai_provider} for chunk {chunk_index + 1} (attempt {attempt + 1})")
                    if self.ai_provider == "Gemini":
                        response = self.config.gemini_model.generate_content(prompt.format(chunk=chunk))
                        if not response or not response.text:
                            logging.error(f"Empty response from {self.ai_provider} for chunk {chunk_index + 1}")
                            continue
                        result = response.text.strip()
                    elif self.ai_provider == "ChatGPT":
                        response = self.config.chatgpt_client.chat.completions.create(
                            model="iwmi-gpt-4o-mini",
                            messages=[{"role": "system", "content": prompt.format(chunk=chunk)}]
                        )
                        if not response or not response.choices:
                            logging.error(f"Empty response from {self.ai_provider} for chunk {chunk_index + 1}")
                            continue
                        result = response.choices[0].message.content.strip()
                    parsed_result = self.extract_json_from_response(result)
                    if parsed_result:
                        return parsed_result
                    else:
                        logging.warning(f"Failed to parse JSON from {self.ai_provider} response for chunk {chunk_index + 1} (attempt {attempt + 1})")
                except Exception as e:
                    logging.error(f"{self.ai_provider} API error for chunk {chunk_index + 1} (attempt {attempt + 1}): {e}")
                    if attempt < 2:
                        interruptable_sleep(2 ** attempt, self.config.running_event)
            logging.error(f"All attempts failed for chunk {chunk_index + 1}")
            return None
        finally:
            with self.config.request_count_lock:
                self.config.active_requests.value -= 1
            self.config.current_gemini_requests.release()

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

    def query_ai_for_semantic_metadata(self, pdf_text, item_id, prompt, features, progress_queue=None):
        chunks = chunk_text_safe(pdf_text, progress_queue=progress_queue, item_id=item_id)
        logging.info(f"Processing {len(chunks)} chunks for semantic extraction for item {item_id}")
        semantic_metadata = {key: default for key, _, default in features}
        successful_chunks = 0
        for i, chunk in enumerate(chunks):
            if not self.config.running_event.is_set() or self.check_timeout():
                logging.info(f"Stopping chunk {i + 1} processing due to stop signal or timeout")
                break
            logging.info(f"Processing chunk {i + 1}/{len(chunks)} for item {item_id}")
            parsed_result = self.query_ai_single_chunk_safe(chunk, item_id, i, prompt, features)
            if parsed_result:
                successful_chunks += 1
                for key, type_desc, default in features:
                    value = parsed_result.get(key, default)
                    if value is None:
                        continue
                    if value == default:
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
            interruptable_sleep(0.1, self.config.running_event)
        logging.info(f"Processed {successful_chunks}/{len(chunks)} chunks for semantic extraction of item {item_id}")
        logging.info(f"Final aggregated metadata: {json.dumps(semantic_metadata, indent=2)[:1000]}...")
        if progress_queue:
            progress_queue.put(f"Analysis completed for item {item_id}")
        return semantic_metadata