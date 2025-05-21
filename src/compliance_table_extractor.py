import logging
from pathlib import Path
import camelot
import numpy as np
import pandas as pd
from tqdm import tqdm

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s')

pdf_folder = Path("pdfs/")


def process_pdf(pdf_filename: str) -> list[pd.DataFrame]:
    """
    Process a PDF file and extract tables into a list of DataFrames.

    Args:
        pdf_filename (str): The path to the PDF file.

    Returns:
        list[pd.DataFrame]: A list of extracted and processed DataFrames.
                             Returns an empty list if an error occurs.
    """
    processed_tables_for_pdf = []
    try:
        # Extract PFI and AUDIT_YEAR from filename
        # Assuming filename format like "Y<year>_AUDIT_REPORT_<pfi>.pdf" or similar
        name_parts = Path(pdf_filename).stem.split("_")
        pfi = name_parts[-1]
        # Attempt to extract year, assuming it's prefixed with 'Y' as in 'Y2022'
        file_year_str = name_parts[0]
        if file_year_str.startswith('Y') and len(file_year_str) > 1:
            file_year = file_year_str[1:]
        else:
            # Fallback or more robust year extraction if needed
            logging.warning(f"Could not determine year from filename: {pdf_filename}, using 'UnknownYear'")
            file_year = "UnknownYear"

        logging.info(f"Processing PDF: {pdf_filename}")
        tables = camelot.read_pdf(pdf_filename, pages="1-end", suppress_stdout=True, line_scale=40)
        logging.info(f"Found {tables.n} tables in {pdf_filename}")

        for i, table in enumerate(tables):
            try:
                df = table.df
                # Assuming the first row is the header
                df.columns = df.iloc[0]
                # Clean column names
                df.columns = [
                    str(c).replace("*", "").replace("\n", "_").replace(" ", "_").upper()
                    for c in df.columns
                ]
                
                # Basic sanity check for expected column, e.g. 'FILE_TYPE'
                if 'FILE_TYPE' not in df.columns:
                    logging.warning(f"Table {i+1} in {pdf_filename} does not have 'FILE_TYPE' column after cleaning. Skipping table.")
                    continue

                # Filter out rows that are likely repeated headers or summary rows
                df = df.loc[df["FILE_TYPE"] != "File\nType", :] # Original filter
                df = df.loc[df["FILE_TYPE"].str.upper() != "FILE_TYPE", :] # More robust filter

                df = df.loc[~df["FILE_TYPE"].str.contains("Total Records Submitted", case=False, na=False), :]
                df = df.replace(r"^\s*$", np.nan, regex=True)

                # Forward fill 'FILE_TYPE' for merged cells
                df["FILE_TYPE"] = df["FILE_TYPE"].ffill()
                
                # Ensure 'PCT_OF_PREVYRAVG_SUBMTD_' column exists before processing
                if "PCT_OF_PREVYRAVG_SUBMTD_" in df.columns:
                    df["PCT_OF_PREVYRAVG_SUBMTD_"] = (
                        df["PCT_OF_PREVYRAVG_SUBMTD_"].astype(str).str.rstrip("%").astype("float") / 100.0
                    )
                else:
                    logging.warning(f"'PCT_OF_PREVYRAVG_SUBMTD_' column not found in table {i+1} of {pdf_filename}. Skipping conversion.")
                    df["PCT_OF_PREVYRAVG_SUBMTD_"] = np.nan # Add as NaN if missing

                df["PFI"] = pfi
                df["AUDIT_YEAR"] = file_year
                
                processed_tables_for_pdf.append(df)
                logging.info(f"Successfully processed table {i+1} from {pdf_filename}")
            except Exception as e:
                logging.error(f"Error processing table {i+1} in {pdf_filename}: {e}. Skipping this table.")
        
        return processed_tables_for_pdf

    except FileNotFoundError:
        logging.error(f"Error processing PDF {pdf_filename}: File not found.")
        return []
    except Exception as e:
        logging.error(f"Error processing PDF {pdf_filename} with camelot: {e}. Skipping this PDF.")
        return []

def extract_compliance_data():
    """
    Main logic to find and process all compliance PDFs from the pdf_folder,
    extract tables, and save a consolidated CSV report.
    Raises RuntimeError if critical steps fail (e.g., no PDFs, no tables extracted).
    """
    logging.info("Starting SPARCS Compliance PDF processing (core logic).")
    
    if not pdf_folder.exists() or not any(pdf_folder.glob("*.pdf")):
        msg = f"PDF input folder {pdf_folder.resolve()} does not exist or is empty. Cannot proceed."
        logging.error(msg)
        raise RuntimeError(msg)

    all_tables_dfs = []
    pdfs_processed_count = 0
    tables_extracted_count = 0

    pdf_list = list(pdf_folder.glob("*.pdf"))
    if not pdf_list: # Should be caught by the check above, but as a safeguard.
        msg = f"No PDF files found in {pdf_folder.resolve()} at the processing stage."
        logging.error(msg) # Changed from warning to error as it's now a critical failure point.
        raise RuntimeError(msg)
    
    logging.info(f"Found {len(pdf_list)} PDF files to process in {pdf_folder.resolve()}.")

    for pdf_file_path in tqdm(pdf_list, desc="Processing Compliance PDFs"):
        try:
            pdf_filename_str = str(pdf_file_path)
            # `process_pdf` already has internal logging for success/failure of individual PDFs/tables
            processed_dfs_from_pdf = process_pdf(pdf_filename_str)
            if processed_dfs_from_pdf:
                all_tables_dfs.extend(processed_dfs_from_pdf)
                tables_extracted_count += len(processed_dfs_from_pdf)
            pdfs_processed_count += 1
        except Exception as e:
            # This catch is for unexpected errors during the loop itself,
            # not within process_pdf (which has its own error handling).
            logging.error(f"Unexpected error during the processing loop for PDF file {pdf_file_path}: {e}", exc_info=True)

    logging.info(f"Finished processing {pdfs_processed_count} PDF(s). Extracted {tables_extracted_count} table(s) in total.")

    if not all_tables_dfs:
        msg = "No tables were extracted from any PDF files. Final report cannot be generated."
        logging.error(msg) # Changed from warning to error
        raise RuntimeError(msg)
    
    try:
        logging.info("Concatenating all extracted tables.")
        final_df = pd.concat(all_tables_dfs, ignore_index=True)
        
        # Perform final filtering
        if "DISCHARGE_MONTH" in final_df.columns:
            final_df = final_df[final_df["DISCHARGE_MONTH"].notna()]
            logging.info("Applied final filtering on 'DISCHARGE_MONTH'.")
        else:
            logging.warning("'DISCHARGE_MONTH' column not found for final filtering. Skipping this filter.")

        output_filename = "SPARCS_Compliance_Report.csv"
        final_df.to_csv(output_filename, index=False)
        logging.info(f"Successfully saved consolidated compliance report to {output_filename}")
    except Exception as e:
        msg = f"Error during final DataFrame concatenation or saving the consolidated report: {e}"
        logging.error(msg, exc_info=True)
        raise RuntimeError(msg) from e

    logging.info("SPARCS Compliance PDF processing (core logic) finished successfully.")
    # return True # Optional


if __name__ == "__main__":
    logging.info("Executing compliance_table_extractor.py as a standalone script.")
    try:
        extract_compliance_data()
        logging.info("compliance_table_extractor.py executed successfully.")
    except RuntimeError as e:
        logging.critical(f"RuntimeError in compliance_table_extractor.py: {e}")
    except Exception as e:
        logging.critical(f"An unexpected critical error occurred in compliance_table_extractor.py: {e}", exc_info=True)
