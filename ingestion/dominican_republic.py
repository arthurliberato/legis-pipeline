# ingestion/dominican_republic.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time
import os
import requests
from urllib.parse import urljoin
from ingestion.base_scraper import BaseScraper

class DominicanRepublicScraper(BaseScraper):
    def __init__(self):
        self.url = "https://camaradediputados.gob.do/asistencia/#145-884-wpfd-2025-primera-legislatura-ordinaria-asistencia"
        self.download_dir = "raw_data/dominican_republic/2025"

    def fetch(self):
        print(f"[FETCH] Loading {self.url}")
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")

        driver = webdriver.Chrome(options=options)
        driver.get(self.url)
        time.sleep(8)  # Allow time for JS to fully load WordPress File Download plugin

        page_source = driver.page_source
        self._download_pdfs(driver)

        driver.quit()
        return page_source

    def _download_pdfs(self, driver):
        print("[DOWNLOAD] Searching for PDFs...")
        os.makedirs(self.download_dir, exist_ok=True)

        links = driver.find_elements(By.TAG_NAME, "a")
        pdf_links = [a.get_attribute("href") for a in links if a.get_attribute("href") and a.get_attribute("href").endswith(".pdf")]

        print(f"[DOWNLOAD] Found {len(pdf_links)} PDFs")

        for i, link in enumerate(pdf_links):
            file_name = link.split("/")[-1].split("?")[0]
            file_path = os.path.join(self.download_dir, file_name)

            if os.path.exists(file_path):
                print(f"[SKIP] Already downloaded: {file_name}")
                continue

            print(f"[DOWNLOAD] {file_name}")
            try:
                r = requests.get(link)
                with open(file_path, "wb") as f:
                    f.write(r.content)
            except Exception as e:
                print(f"[ERROR] Failed to download {link}: {e}")

    def parse(self, raw_data):
        # For now, return dummy placeholder
        return {"status": "PDFs downloaded"}

    def save(self, data, output_path):
        print("[SAVE] Skipping CSV save until PDF parsing is implemented.")

if __name__ == "__main__":
    scraper = DominicanRepublicScraper()
    scraper.run("raw_data/dominican_republic_attendance_placeholder.csv")
