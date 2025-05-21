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
        return

    try:
        # Parse the HTML using BeautifulSoup
        soup = BeautifulSoup(response.content, "html.parser")

        # Find the table with the data
        table = soup.find("table", class_="table")
        if not table:
            logging.warning(f"No table with class 'table' found on {report_url}")
            return

        # Extract the header row
        header_row = [th.text.strip() for th in table.find_all("th")]

        # Add the two new columns to the header
        header_row.extend(["Report Type", "Date Published"])

        # Extract data rows
        data_rows = []

        # Attempt to find report_type and date_published on the report page
        # These selectors might need adjustment if the report page structure differs significantly.
        report_type_tag = soup.find("td", class_="c systemtitle3")
        date_published_tag = soup.find("td", class_="r systemtitle4")

        report_type = report_type_tag.text.strip() if report_type_tag else "Unknown Report Type"
        date_published = date_published_tag.text.strip() if date_published_tag else "Unknown Date"

        for tr in table.find_all("tr")[1:]:  # Skip the header row
            row = [td.text.strip() for td in tr.find_all("td")]
            # Add the new columns to the data row
            row.extend([report_type, date_published])
            data_rows.append(row)

        if not data_rows:
            logging.warning(f"No data rows found in the table on {report_url}")
            return

        # Create a Pandas DataFrame
        df = pd.DataFrame(data_rows, columns=header_row)

        # Ensure 'output' directory exists
        output_dir = "output"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            logging.info(f"Created directory: {output_dir}")

        # Save the DataFrame to a CSV file in the 'output' directory
        csv_filename = os.path.join(output_dir, f"{report_name_info}.csv")
        df.to_csv(csv_filename, index=False)
        logging.info(f"Successfully saved data to {csv_filename}")

    except Exception as e:
        logging.error(f"Error parsing {report_url}: {e}")


if __name__ == "__main__":
    logging.info("Starting audit report extraction process.")
    # Get the HTML content of the webpage
    main_url = "https://www.health.ny.gov/statistics/sparcs/reports/"
    
    try:
        logging.info(f"Fetching main listing page: {main_url}")
        response = requests.get(main_url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to fetch main listing page {main_url}: {e}")
        exit()

    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Filter links containing the word "audit" and expand relative URLs
    audit_links = []
    for a in soup.find_all("a", href=True):
        link_href = a["href"]
        if "audit" in link_href:
            full_url = urljoin(main_url, link_href)  # Expand relative URLs
            audit_links.append(full_url)

    if not audit_links:
        logging.warning(f"No audit links found on {main_url}")
    else:
        logging.info(f"Found {len(audit_links)} audit links.")

    for audit_link in audit_links:
        # Generate report_name_info from the link
        # e.g., https://www.health.ny.gov/statistics/sparcs/reports/audit/some_report_2023.htm -> some_report_2023
        try:
            report_name_parts = audit_link.split('/')
            filename_with_ext = report_name_parts[-1] if report_name_parts[-1] else report_name_parts[-2]
            report_name_info = os.path.splitext(filename_with_ext)[0]
            if not report_name_info: # Handle cases like /audit/
                 report_name_info = f"audit_report_{audit_links.index(audit_link)}"

        except Exception as e:
            logging.error(f"Error generating report name for {audit_link}: {e}")
            report_name_info = f"unknown_report_{audit_links.index(audit_link)}"

        logging.info(f"Processing report: {audit_link} with name: {report_name_info}")
        parse_audit_report(audit_link, report_name_info)
    
    logging.info("Finished audit report extraction process.")
