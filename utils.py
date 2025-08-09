import logging
import time

def interruptable_sleep(duration, event):
    start = time.time()
    while time.time() - start < duration:
        if not event.is_set():
            logging.info("Sleep interrupted due to stop signal")
            return False
        time.sleep(min(1, duration - (time.time() - start))) 
    return True

def chunk_text_safe(text, chunk_size=3000, overlap=150, progress_queue=None, item_id=None):
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