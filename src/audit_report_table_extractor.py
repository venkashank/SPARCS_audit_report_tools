import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def parse_audit_report(url):
    response = requests.get(url)
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Find the table with the data
    table = soup.find("table", class_="table")

    # Extract the header row
    header_row = [th.text.strip() for th in table.find_all("th")]

    # Add the two new columns to the header
    header_row.extend(["Report Type", "Date Published"])

    # Extract data rows
    data_rows = []
    for tr in table.find_all("tr")[1:]:  # Skip the header row
        row = [td.text.strip() for td in tr.find_all("td")]

        # Get the text from the specified HTML tags
        report_type = soup.find("td", class_="c systemtitle3").text.strip()
        date_published = soup.find("td", class_="r systemtitle4").text.strip()

        # Add the new columns to the data row
        row.extend([report_type, date_published])

        data_rows.append(row)

    # Create a Pandas DataFrame
    df = pd.DataFrame(data_rows, columns=header_row)

    # Save the DataFrame to a CSV file
    df.to_csv(f"{report_type}.csv", index=False)


if __name__ == "__main__":
    # Get the HTML content of the webpage
    url = "https://www.health.ny.gov/statistics/sparcs/reports/"  # Replace with the actual URL
    response = requests.get(url)
    # Parse the HTML using BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Filter links containing the word "audit" and expand relative URLs
    audit_links = []
    for a in soup.find_all("a", href=True):
        if "audit" in a["href"]:
            full_url = urljoin(url, a["href"])  # Expand relative URLs
            audit_links.append(full_url)

    # Print the filtered list of links with full URLs
    print(audit_links)
    for audit_link in audit_links:
        parse_audit_report(audit_link)
