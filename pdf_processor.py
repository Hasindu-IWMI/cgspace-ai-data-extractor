import fitz
import logging
import tempfile
import os
import time
from utils import interruptable_sleep  # If needed
from api_client import APIClient

class PDFProcessor:
    def __init__(self, config):
        self.config = config

    def extract_pdf_text_safe(self, pdf_path, progress_queue=None, item_id=None, chunk_queue=None):
        try:
            logging.info(f"Extracting text from file {pdf_path}")
            pdf = fitz.open(pdf_path)
            text = ""
            page_count = len(pdf)
            logging.info(f"PDF has {page_count} pages")
            for page_num, page in enumerate(pdf):
                if not self.config.running_event.is_set():
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
                        chunk_queue.put(f"Extracting text from PDF for item {item_id}: {progress_percent}%")
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

    def download_pdf_safe(self, pdf_url, filename, page_num, item_id, prompt, features, ai_provider, ai_model, progress_queue, ai_handler, chunk_queue=None):
        if not self.config.running_event.is_set():
            logging.info(f"Stopping PDF download for {pdf_url} due to stop signal")
            return None, None
        semantic_metadata = None
        tmp_path = None
        try:
            api_client = APIClient(self.config)
            response = api_client.make_api_request_safe(pdf_url, stream=True, timeout=10)
            total_size = int(response.headers.get('content-length', 0))
            downloaded_size = 0
            tmp_path = tempfile.mktemp(suffix='.pdf')
            with open(tmp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if not self.config.running_event.is_set():
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
                            if chunk_queue: 
                                chunk_queue.put(f"Downloading PDF for item {item_id}: {int(progress * 100)}%")
                        else:
                            progress_queue.put(f"Downloading PDF for item {item_id}: {downloaded_size} bytes")
                            if chunk_queue: 
                                chunk_queue.put(f"Downloading PDF for item {item_id}: {downloaded_size} bytes")

            progress_queue.put(f"Download completed for item {item_id}")
            if chunk_queue:
                chunk_queue.put(f"Download completed for item {item_id}")

            if not self.config.running_event.is_set():
                logging.info(f"Stopping PDF text extraction for {pdf_url} due to stop signal")
                return None, None
            pdf_text = self.extract_pdf_text_safe(tmp_path, progress_queue=progress_queue, item_id=item_id,chunk_queue=chunk_queue)
            progress_queue.put(f"Extraction completed for item {item_id}")
            if chunk_queue:  
                chunk_queue.put(f"Extraction completed for item {item_id}")

            if not self.config.running_event.is_set():
                logging.info(f"Stopping semantic processing for {pdf_url} due to stop signal")
                return None, None
            semantic_metadata = ai_handler.query_ai_for_semantic_metadata(pdf_text, item_id, prompt, features, progress_queue=progress_queue)
        except Exception as e:
            logging.error(f"Failed to process PDF {pdf_url}: {e}")
            progress_queue.put(f"Error processing PDF for item {item_id}: {str(e)}")
            if chunk_queue: 
                chunk_queue.put(f"Error processing PDF for item {item_id}: {str(e)}")

        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.unlink(tmp_path)
            return None, semantic_metadata