import logging
from typing import List
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO)

REPORTS_URL = (
    "https://www.health.ny.gov/statistics/sparcs/reports/compliance/pfi_facilities.htm"
)

def extract_pdf_urls(url: str) -> List[str]:
    """
    Extracts the URLs of PDF files from a given URL.

    Args:
        url (str): The URL to scrape for PDF URLs.

    Returns:
        List[str]: A list of extracted PDF URLs.
    """
    logging.info(f"Extracting PDF URLs from {url}")
    r = requests.get(url).text
    soup = BeautifulSoup(r, "html.parser")
    pdf_urls: List[str] = []
    for link in soup.findAll(
        "a", attrs={"href": re.compile("^/statistics/sparcs/reports/compliance/2")}
    ):
        pdf_urls.append(link.get("href"))
    logging.info(f"Extracted {len(pdf_urls)} PDF URLs")
    return pdf_urls

if __name__ == "__main__":
    logging.info("Starting PDF download process")

    pdf_urls: List[str] = extract_pdf_urls(REPORTS_URL)
    p = Path("pdfs/")
    p.mkdir(parents=True, exist_ok=True)

    for url in tqdm(pdf_urls):
        file_name:Path = Path(url.split("/")[-1])
        file_path:Path = p / file_name
        logging.info(f"Downloading PDF from {url}")
        with open(file_path, "wb") as f:
            f.write(requests.get(f"https://www.health.ny.gov/{url}").content)
        logging.info(f"Downloaded PDF to {file_path}")
