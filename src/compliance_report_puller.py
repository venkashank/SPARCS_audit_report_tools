import logging
from typing import List
import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from pathlib import Path
from urllib.parse import urljoin

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

REPORTS_URL = (
    "https://www.health.ny.gov/statistics/sparcs/reports/compliance/pfi_facilities.htm"
)

def extract_pdf_urls(url: str) -> List[str]:
    """
    Extracts the URLs of PDF files from a given URL.

    Args:
        url (str): The URL to scrape for PDF URLs.

    Returns:
        List[str]: A list of extracted PDF URLs. Returns an empty list if an error occurs.
    """
    logging.info(f"Attempting to extract PDF URLs from {url}")
    try:
        response = requests.get(url, timeout=10) # Added timeout
        response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
        soup = BeautifulSoup(response.text, "html.parser")
        pdf_urls: List[str] = []
        # Regex updated to be more specific to PDF links starting with a year in 2xxx
        for link in soup.findAll(
            "a", attrs={"href": re.compile(r"^/statistics/sparcs/reports/compliance/2\d{3}.*\.pdf$", re.IGNORECASE)}
        ):
            pdf_path = link.get("href")
            if pdf_path: # Ensure href is not None
                # Construct absolute URL if it's relative
                full_pdf_url = urljoin(url, pdf_path)
                pdf_urls.append(full_pdf_url)
        logging.info(f"Successfully extracted {len(pdf_urls)} PDF URLs from {url}")
        return pdf_urls
    except requests.exceptions.RequestException as e:
        logging.error(f"Error extracting PDF URLs from {url}: {e}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while extracting PDF URLs from {url}: {e}")
        return []

if __name__ == "__main__":
    logging.info("Starting PDF download process")

    pdf_urls: List[str] = extract_pdf_urls(REPORTS_URL)
    
    if not pdf_urls:
        logging.warning("No PDF URLs extracted. Exiting PDF download process.")
        exit()
        
    output_pdf_dir = Path("pdfs/")
    output_pdf_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"PDFs will be saved to {output_pdf_dir.resolve()}")

    successful_downloads = 0
    failed_downloads = 0

    for pdf_url in tqdm(pdf_urls, desc="Downloading PDFs"):
        try:
            # Derive filename from the URL path
            file_name_str = Path(pdf_url.split("/")[-1]).name
            if not file_name_str.lower().endswith(".pdf"):
                logging.warning(f"Skipping URL as it does not appear to be a PDF: {pdf_url}")
                failed_downloads +=1
                continue

            file_path:Path = output_pdf_dir / file_name_str
            
            logging.debug(f"Attempting to download PDF from {pdf_url}")
            
            pdf_response = requests.get(pdf_url, timeout=30) # Added timeout
            pdf_response.raise_for_status() # Raise an exception for bad status codes

            with open(file_path, "wb") as f:
                f.write(pdf_response.content)
            logging.info(f"Successfully downloaded PDF to {file_path}")
            successful_downloads += 1
        except requests.exceptions.RequestException as e:
            logging.error(f"Error downloading PDF from {pdf_url}: {e}")
            failed_downloads += 1
        except Exception as e:
            logging.error(f"An unexpected error occurred while downloading {pdf_url}: {e}")
            failed_downloads += 1
            
    logging.info(f"PDF download process finished. Successful downloads: {successful_downloads}, Failed downloads: {failed_downloads}")
