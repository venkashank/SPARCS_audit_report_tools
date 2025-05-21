import os
import sys

if __name__ == "__main__" and __package__ is None:
    # This block executes if the script is run directly (e.g., python src/main.py)
    # and not as part of a package (e.g., not python -m src.main)

    # Get the absolute path of the 'src' directory (where main.py resides)
    src_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Get the absolute path of the project root directory (parent of 'src')
    project_root = os.path.dirname(src_dir)
    
    # Add the project root to sys.path if it's not already there
    # This allows Python to find the 'src' package
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
    
    # Set __package__ to 'src'
    # This informs Python that main.py is part of the 'src' package,
    # which is necessary for relative imports (from .module import ...) to work.
    __package__ = "src"

# --- Original script content starts below ---
import logging
# No need for sys and os if not using sys.path modifications here (already imported above)

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
    # This error logging might be less relevant now if the __package__ fix works,
    # but keeping it doesn't hurt, as it might catch other import issues.
    logging.error(f"Error importing modules: {e}. This might indicate an issue beyond the sys.path/package fix.")
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
    # The code block at the top (if __name__ == "__main__" and __package__ is None:)
    # will have already run and set up sys.path and __package__ if this script
    # is executed directly (e.g., `python src/main.py`).
    # This allows the relative imports (`from .module ...`) to work.
    
    # Running as `python -m src.main` would set `__package__` correctly by default,
    # and the top block wouldn't modify `__package__`.
    run_pipeline()
