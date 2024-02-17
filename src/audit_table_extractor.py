import logging
from pathlib import Path

import camelot
import numpy as np
import pandas as pd
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO)

pdf_list = Path("pdfs/").glob("*.pdf")


def process_pdf(pdf_filename: str, folder: str):
    """
    Process a PDF file and extract tables into CSV files.

    Args:
        pdf_filename (str): The path to the PDF file.
        folder (str): The folder where the CSV files will be saved.

    Returns:
        None
    """
    csv_name = pdf_filename.split("/")[-1].rstrip(".pdf")
    pfi = csv_name.split("_")[-1]
    file_year = pdf_filename.split("_")[0].split("/")[-1][1:]
    tables = camelot.read_pdf(pdf_filename, pages="1-end")
    table_counter = 1
    for table_counter, table in enumerate(tables):
        df = table.df
        df.columns = df.iloc[0]
        df.columns = [
            c.replace("*", "").replace("\n", "_").replace(" ", "_").upper()
            for c in df.columns
        ]

        df = df.loc[df["FILE_TYPE"] != "File\nType", :]

        df = df.loc[~df["FILE_TYPE"].str.match("Total Records Submitted"), :]
        df = df.replace(r"^\s*$", np.nan, regex=True)

        df["FILE_TYPE"] = df["FILE_TYPE"].ffill()

        df["PCT_OF_PREVYRAVG_SUBMTD_"] = (
            df["PCT_OF_PREVYRAVG_SUBMTD_"].str.rstrip("%").astype("float") / 100.0
        )

        df["PFI"] = pfi

        df["AUDIT_YEAR"] = file_year

        csv_filename = f"{folder}/{csv_name}{table_counter}.csv"
        df.to_csv(csv_filename, index=False)
        logging.info(f"Saved table {table_counter} to {csv_filename}")


if __name__ == "__main__":
    p = Path("csvs/")
    p.mkdir(parents=True, exist_ok=True)
    for pdf in tqdm(pdf_list):
        process_pdf(str(pdf), "csvs")
    df_list = []
    for csv_file in Path("csvs/").glob("*.csv"):
        df = pd.read_csv(csv_file, dtype=str)
        df_list.append(df)
    df = pd.concat(df_list)
    df = df[df["DISCHARGE_MONTH"].notna()]
    df.to_csv("SPARCS_Compliance_Report.csv", index=False)
    logging.info("Saved SPARCS Compliance Report to SPARCS_Compliance_Report.csv")
