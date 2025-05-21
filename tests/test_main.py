import sys
import os
import unittest
from unittest.mock import patch, Mock, call, ANY # ANY is useful for logging calls
import logging

# Add src directory to sys.path to allow direct import of modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

# Now we can import run_pipeline from main
# The try-except for ImportError in main.py itself should handle if this structure is wrong
# when main.py is run, but for tests, this direct import should work given sys.path.
from main import run_pipeline

# Disable logging for tests specifically for main.py unless testing log messages
# This prevents cluttering test output with pipeline logs.
# Individual tests can enable/assert logs if needed.
# logging.disable(logging.CRITICAL)


class TestMainPipeline(unittest.TestCase):

    def setUp(self):
        # This helps to re-enable logging if a specific test disables it or changes level
        logging.disable(logging.NOTSET)

    @patch('main.extract_audit_data')
    @patch('main.extract_compliance_data')
    @patch('main.pull_compliance_pdfs')
    @patch('main.logging.info') # To check log messages
    def test_pipeline_successful_run_and_call_sequence(
        self, mock_log_info, mock_pull_pdfs, mock_extract_data, mock_extract_audit
    ):
        """
        Tests that all pipeline functions are called in sequence during a successful run
        and that a success message is logged.
        """
        # Using a manager mock to check call order easily
        manager_mock = Mock()
        manager_mock.attach_mock(mock_pull_pdfs, 'pull_pdfs_call')
        manager_mock.attach_mock(mock_extract_data, 'extract_data_call')
        manager_mock.attach_mock(mock_extract_audit, 'extract_audit_call')

        run_pipeline()

        expected_calls = [
            call.pull_pdfs_call(),
            call.extract_data_call(),
            call.extract_audit_call()
        ]
        self.assertEqual(manager_mock.method_calls, expected_calls)

        mock_pull_pdfs.assert_called_once()
        mock_extract_data.assert_called_once()
        mock_extract_audit.assert_called_once()
        
        # Check for the final success log message
        mock_log_info.assert_any_call("SPARCS data processing pipeline finished successfully.")


    @patch('main.extract_audit_data')
    @patch('main.extract_compliance_data')
    @patch('main.pull_compliance_pdfs', side_effect=RuntimeError("PDF Pull Failed"))
    @patch('main.logging.error') # To check error log messages
    def test_pipeline_stops_if_pull_pdfs_fails(
        self, mock_log_error, mock_pull_pdfs, mock_extract_data, mock_extract_audit
    ):
        """Tests that the pipeline stops if pull_compliance_pdfs fails."""
        run_pipeline()

        mock_pull_pdfs.assert_called_once()
        mock_extract_data.assert_not_called()
        mock_extract_audit.assert_not_called()
        
        mock_log_error.assert_any_call(
            "Step 1: Failed to download compliance PDF reports. Error: PDF Pull Failed",
            exc_info=True
        )
        mock_log_error.assert_any_call("SPARCS data processing pipeline finished with errors. Not all steps were successful.")


    @patch('main.extract_audit_data')
    @patch('main.extract_compliance_data', side_effect=RuntimeError("Data Extraction Failed"))
    @patch('main.pull_compliance_pdfs')
    @patch('main.logging.error')
    def test_pipeline_stops_if_extract_data_fails(
        self, mock_log_error, mock_pull_pdfs, mock_extract_data, mock_extract_audit
    ):
        """Tests that the pipeline stops if extract_compliance_data fails."""
        run_pipeline()

        mock_pull_pdfs.assert_called_once()
        mock_extract_data.assert_called_once()
        mock_extract_audit.assert_not_called()

        mock_log_error.assert_any_call(
            "Step 2: Failed to extract compliance data. Error: Data Extraction Failed",
            exc_info=True
        )
        mock_log_error.assert_any_call("SPARCS data processing pipeline finished with errors. Not all steps were successful.")


    @patch('main.extract_audit_data', side_effect=RuntimeError("Audit Extraction Failed"))
    @patch('main.extract_compliance_data')
    @patch('main.pull_compliance_pdfs')
    @patch('main.logging.error')
    def test_pipeline_logs_error_if_extract_audit_fails(
        self, mock_log_error, mock_pull_pdfs, mock_extract_data, mock_extract_audit
    ):
        """
        Tests that the pipeline attempts all steps but logs an error
        if extract_audit_data (the last step) fails.
        """
        run_pipeline()

        mock_pull_pdfs.assert_called_once()
        mock_extract_data.assert_called_once()
        mock_extract_audit.assert_called_once()
        
        mock_log_error.assert_any_call(
            "Step 3: Failed to process audit report HTML tables. Error: Audit Extraction Failed",
            exc_info=True
        )
        mock_log_error.assert_any_call("SPARCS data processing pipeline finished with errors. Not all steps were successful.")

    @patch('main.extract_audit_data')
    @patch('main.extract_compliance_data')
    @patch('main.pull_compliance_pdfs')
    @patch('main.logging.info')
    @patch('main.logging.error')
    def test_unexpected_exception_in_step_1(self, mock_log_error, mock_log_info, mock_pull_pdfs, mock_extract_data, mock_extract_audit):
        """Test that a non-RuntimeError exception in step 1 is handled and stops the pipeline."""
        mock_pull_pdfs.side_effect = ValueError("Unexpected Value Error")
        run_pipeline()

        mock_pull_pdfs.assert_called_once()
        mock_extract_data.assert_not_called()
        mock_extract_audit.assert_not_called()
        mock_log_error.assert_any_call(
            "Step 1: An unexpected error occurred during compliance PDF download. Error: Unexpected Value Error",
            exc_info=True
        )
        mock_log_error.assert_any_call("SPARCS data processing pipeline finished with errors. Not all steps were successful.")


if __name__ == '__main__':
    # Important: If main.py uses `from .module import func`, then running test_main.py directly
    # might cause ImportError if 'src' is not properly in PYTHONPATH or recognized as a package.
    # Running tests via `python -m unittest discover tests` from the project root is preferred.
    unittest.main()
