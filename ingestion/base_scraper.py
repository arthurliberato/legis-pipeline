# ingestion/base_scraper.py

import abc

class BaseScraper(abc.ABC):
    """
    Abstract base class for country-specific attendance scrapers.
    """

    @abc.abstractmethod
    def fetch(self):
        """
        Fetch raw content from the source (HTML, JSON, etc.)
        Should return the raw content for parsing.
        """
        pass

    @abc.abstractmethod
    def parse(self, raw_data):
        """
        Parse the fetched raw content into a structured format (e.g. pandas DataFrame).
        """
        pass

    @abc.abstractmethod
    def save(self, parsed_data, output_path):
        """
        Save the parsed data to a local file (CSV, JSON, etc.).
        """
        pass

    def run(self, output_path):
        """
        High-level method that runs the full scrape process.
        """
        print("Starting scraping workflow...")
        raw = self.fetch()
        print("Data fetched.")
        parsed = self.parse(raw)
        print("Data parsed.")
        self.save(parsed, output_path)
        print(f"Data saved to {output_path}")
