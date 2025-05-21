import logging
# No need for sys and os if not using sys.path modifications here

# Configure logging (consistent with other scripts)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
)

# Import the main functions from the refactored scripts
try:
    from .compliance_report_puller import pull_compliance_pdfs
    from .compliance_table_extractor import extract_compliance_data
    from .audit_report_table_extractor import extract_audit_data
except ImportError as e:
    logging.error(f"Error importing modules: {e}. Ensure this script is run as part of a package, e.g., 'python -m src.main'.")
    # Optionally, re-raise or sys.exit if imports are critical for any script functionality
    # For now, we'll let it proceed, and it will fail later if functions are not defined.
    # However, it's better to make these imports robust or guide the user.
    # For this task, assume the imports will work when run correctly.
    # If running `python src/main.py` directly, relative imports will fail.
    # It should be run as `python -m src.main` from the project root directory.
    # Adding a check here to prevent running if imports fail.
    raise


def run_pipeline():
    """
    Runs the full SPARCS data collection and processing pipeline sequentially.
    The pipeline stops if any critical step fails.
    """
    logging.info("Starting the SPARCS data processing pipeline...")
    all_steps_successful = True

    # --- Step 1: Download Compliance PDF Reports ---
    if all_steps_successful: # This initial check is redundant but maintains pattern
        try:
            logging.info("Step 1: Downloading compliance PDF reports...")
            pull_compliance_pdfs()
            logging.info("Step 1: Compliance PDF reports download completed successfully.")
        except RuntimeError as e: # Catching RuntimeError as raised by the refactored script
            logging.error(f"Step 1: Failed to download compliance PDF reports. Error: {e}", exc_info=True)
            all_steps_successful = False
        except Exception as e: # Catch any other unexpected errors
            logging.error(f"Step 1: An unexpected error occurred during compliance PDF download. Error: {e}", exc_info=True)
            all_steps_successful = False

    # --- Step 2: Extract and Process Tables from Compliance PDFs ---
    if all_steps_successful:
        try:
            logging.info("Step 2: Extracting and processing tables from compliance PDFs...")
            extract_compliance_data()
            logging.info("Step 2: Compliance PDF table extraction completed successfully.")
        except RuntimeError as e:
            logging.error(f"Step 2: Failed to extract compliance data. Error: {e}", exc_info=True)
            all_steps_successful = False
        except Exception as e:
            logging.error(f"Step 2: An unexpected error occurred during compliance data extraction. Error: {e}", exc_info=True)
            all_steps_successful = False

    # --- Step 3: Process Audit Report HTML Tables ---
    if all_steps_successful:
        try:
            logging.info("Step 3: Processing audit report HTML tables...")
            extract_audit_data()
            logging.info("Step 3: Audit report HTML table processing completed successfully.")
        except RuntimeError as e:
            logging.error(f"Step 3: Failed to process audit report HTML tables. Error: {e}", exc_info=True)
            all_steps_successful = False
        except Exception as e:
            logging.error(f"Step 3: An unexpected error occurred during audit report processing. Error: {e}", exc_info=True)
            all_steps_successful = False

    if all_steps_successful:
        logging.info("SPARCS data processing pipeline finished successfully.")
    else:
        logging.error("SPARCS data processing pipeline finished with errors. Not all steps were successful.")

if __name__ == "__main__":
    # This allows src/main.py to be run directly for development/testing,
    # but for relative imports to work correctly, it's better to run as a module:
    # python -m src.main (from the parent directory of src)
    # If run as `python src/main.py`, and if the other files are in the same dir,
    # you might need to adjust imports or sys.path.
    # Given the `from .script import func` syntax, it assumes `src` is a package.
    run_pipeline()
