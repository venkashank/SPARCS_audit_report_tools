import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')


def parse_audit_report(report_url, report_name_info):
    """
    Parses an audit report page, extracts table data, and saves it to a CSV file.

    Args:
        report_url (str): The URL of the audit report page.
        report_name_info (str): A unique identifier for the report, used for the CSV filename.
    """
    try:
        logging.info(f"Fetching data from {report_url}")
        response = requests.get(report_url)
        response.raise_for_status()  # Raise an exception for bad status codes
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching {report_url}: {e}")
        return False # Indicate failure

    try:
        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the table with the data
        table = soup.find("table", class_="table")
        if not table:
            logging.warning(f"No table with class 'table' found on {report_url}")
            return False # Indicate failure: no table found

        # Extract the header row
        header_row = [th.text.strip() for th in table.find_all("th")]

        # Add the two new columns to the header
        header_row.extend(["Report Type", "Date Published"])

        # Extract data rows
        data_rows = []

        # Attempt to find report_type and date_published on the report page
        report_type_tag = soup.find("td", class_="c systemtitle3")
        date_published_tag = soup.find("td", class_="r systemtitle4")

        report_type = report_type_tag.text.strip() if report_type_tag else "Unknown Report Type"
        date_published = date_published_tag.text.strip() if date_published_tag else "Unknown Date"

        for tr in table.find_all("tr")[1:]:  # Skip the header row
            row = [td.text.strip() for td in tr.find_all("td")]
            row.extend([report_type, date_published])
            data_rows.append(row)

        if not data_rows:
            logging.warning(f"No data rows found in the table on {report_url}")
            return False # Indicate failure: no data rows

        # Create a Pandas DataFrame
        df = pd.DataFrame(data_rows, columns=header_row)

        # Ensure 'output' directory exists
        output_dir = Path("output") # Using Path object for consistency
        output_dir.mkdir(parents=True, exist_ok=True) # Idempotent

        # Save the DataFrame to a CSV file in the 'output' directory
        csv_filename = output_dir / f"{report_name_info}.csv"
        df.to_csv(csv_filename, index=False)
        logging.info(f"Successfully saved data to {csv_filename}")
        return True # Indicate success

    except OSError as e: # Catch errors related to file system operations specifically
        logging.error(f"OS error during parsing or saving for {report_url} (e.g., creating directory, saving file): {e}")
        return False
    except Exception as e:
        logging.error(f"Error parsing {report_url} or saving CSV: {e}", exc_info=True)
        return False # Indicate general failure


# Define main_url at module level or pass as argument if it needs to be configurable
MAIN_AUDIT_URL = "https://www.health.ny.gov/statistics/sparcs/reports/"

def extract_audit_data(base_url=MAIN_AUDIT_URL):
    """
    Main logic to fetch the main listing page, find audit report links,
    and process each linked page to extract HTML table data.
    Raises RuntimeError if critical steps fail (e.g., cannot fetch main page, no links found, no data extracted).
    """
    logging.info("Starting audit report HTML table extraction process (core logic).")
    
    try:
        logging.info(f"Fetching main listing page for audit reports: {base_url}")
        response = requests.get(base_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        msg = f"Failed to fetch main listing page {base_url}: {e}"
        logging.error(msg, exc_info=True)
        raise RuntimeError(msg) from e

    soup = BeautifulSoup(response.content, "html.parser")
    audit_links = []
    for a_tag in soup.find_all("a", href=True):
        link_href = a_tag["href"]
        # Assuming "audit" in href is the primary indicator. This might need refinement
        # if there are other non-HTML audit report links.
        if "audit" in link_href and (link_href.endswith(".htm") or link_href.endswith(".html") or not Path(link_href).suffix):
            full_url = urljoin(base_url, link_href)
            audit_links.append(full_url)

    if not audit_links:
        msg = f"No audit report links found on {base_url}. Cannot proceed."
        logging.error(msg) # Changed from warning
        raise RuntimeError(msg)
    else:
        logging.info(f"Found {len(audit_links)} potential audit report links.")

    successful_extractions = 0
    for i, audit_link_url in enumerate(audit_links):
        try:
            # Generate report_name_info from the link
            # e.g., https://.../audit/some_report_2023.htm -> some_report_2023
            path_part = Path(audit_link_url.split("?")[0]) # Remove query params for name
            report_name_info = path_part.stem
            if not report_name_info or report_name_info == audit_link_url: # if stem is empty or same as full url (e.g. just domain)
                report_name_info = f"audit_report_{i+1}" # Fallback name
        except Exception as e:
            logging.error(f"Error generating report name for {audit_link_url}: {e}")
            report_name_info = f"unknown_audit_report_{i+1}"

        logging.info(f"Processing audit report from: {audit_link_url} as '{report_name_info}'")
        if parse_audit_report(audit_link_url, report_name_info):
            successful_extractions += 1
    
    logging.info(f"Finished processing all audit report links. Successfully extracted data for {successful_extractions} out of {len(audit_links)} links.")

    if successful_extractions == 0 and len(audit_links) > 0:
        msg = "No data was successfully extracted from any of the audit report links."
        logging.error(msg)
        raise RuntimeError(msg)
    
    # return True # Optional

if __name__ == "__main__":
    logging.info("Executing audit_report_table_extractor.py as a standalone script.")
    try:
        extract_audit_data()
        logging.info("audit_report_table_extractor.py executed successfully.")
    except RuntimeError as e:
        logging.critical(f"RuntimeError in audit_report_table_extractor.py: {e}")
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in audit_report_table_extractor.py: {e}", exc_info=True)
