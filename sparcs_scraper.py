import requests
from bs4 import BeautifulSoup
import re
from tqdm import tqdm
from pathlib import Path
import os

_CWD = os.getcwd()

_WEBURL = (
    "https://www.health.ny.gov/statistics/sparcs/reports/compliance/pfi_facilities.htm"
)


if __name__ == "__main__":
    r = requests.get(_WEBURL).text
    soup = BeautifulSoup(r, "html.parser")
    pdf_urls = []
    for link in soup.findAll(
        "a", attrs={"href": re.compile("^/statistics/sparcs/reports/compliance/2")}
    ):
        pdf_urls.append(link.get("href"))

    for url in tqdm(pdf_urls):
        file_name = url.split("/")[-1]
        with open(file_name, "wb") as f:
            f.write(requests.get(f"https://www.health.ny.gov/{url}").content)

    p = Path("pdfs/")
    p.mkdir(parents=True, exist_ok=True)
    for file in Path(_CWD).glob("*.pdf"):
        folder_name = file.stem.rpartition("_")[-1]
        file.rename(Path(_CWD) / "pdfs" / file.name)
