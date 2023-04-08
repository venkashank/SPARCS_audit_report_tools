import camelot
from pathlib import Path
from tqdm import tqdm
import numpy as np
import pandas as pd


pdf_list = Path("pdfs/").glob("*.pdf")


def process_pdf(pdf_filename: str, folder: str):
    csv_name = pdf_filename.split("/")[-1].rstrip(".pdf")
    pfi = csv_name.split("_")[-1]
    file_year = pdf_filename.split("_")[0].split("/")[-1][1:]
    tables = camelot.read_pdf(pdf_filename)
    table_counter = 1
    for table in tables:
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

        df.to_csv(f"{folder}/{csv_name}{table_counter}.csv", index=False)
        table_counter += 1


if __name__ == "__main__":
    for pdf in tqdm(pdf_list):
        process_pdf(str(pdf), "csvs")
    df_list = []
    for csv_file in Path("csvs/").glob("*.csv"):
        df = pd.read_csv(csv_file, dtype=str)
        df_list.append(df)
    df = pd.concat(df_list)
    df = df[df["DISCHARGE_MONTH"].notna()]
    df.to_csv("SPARCS_AUDIT_REPORT.csv", index=False)
