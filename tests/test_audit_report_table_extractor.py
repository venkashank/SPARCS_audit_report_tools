import sys
import os
import unittest
from unittest.mock import patch, MagicMock, ANY
import pandas as pd
import requests # For requests.exceptions.RequestException

# Add src directory to sys.path to allow direct import of modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.audit_report_table_extractor import parse_audit_report
from pathlib import Path as TestPath # Use TestPath to avoid conflict with module's Path

class TestAuditReportTableExtractor(unittest.TestCase):

    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True)
    @patch('src.audit_report_table_extractor.os.makedirs') # Mock makedirs
    @patch('src.audit_report_table_extractor.os.path.exists') # Mock path.exists
    def test_parse_audit_report_html_parsing(self, mock_path_exists, mock_makedirs, mock_to_csv, mock_requests_get):
        """Test successful HTML parsing, DataFrame creation, and metadata addition."""
        mock_path_exists.return_value = True # Assume output directory exists

        sample_html = """
        <html><body>
            <td class="c systemtitle3">Test Report Type Value</td>
            <td class="r systemtitle4">Jan 1, 2024</td>
            <table class="table">
                <tr><th>Col1</th><th>Col2</th></tr>
                <tr><td>R1C1</td><td>R1C2</td></tr>
                <tr><td>R2C1</td><td>R2C2</td></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock() # Mock to avoid actual HTTP error checking
        mock_requests_get.return_value = mock_response

        parse_audit_report("http://fakeurl.com/report.html", "test_report_name")

        mock_requests_get.assert_called_once_with("http://fakeurl.com/report.html")
        mock_to_csv.assert_called_once()
        
        # Check the DataFrame passed to to_csv
        call_args = mock_to_csv.call_args
        self.assertIsNotNone(call_args, "to_csv was not called")
        
        # The way to get the DataFrame instance when a method `df.to_csv(path)` is mocked via
        # `@patch('pandas.DataFrame.to_csv')` is that the mock replaces `to_csv`.
        # The first argument to the mock *is the DataFrame instance itself* (`self`).
        # The second argument is the first argument passed to `to_csv` (i.e., `path`).
        # So, df_instance = mock_to_csv.call_args.args[0]
        # And path_argument = mock_to_csv.call_args.args[1]

from pathlib import Path as TestPath # Use TestPath to avoid conflict with module's Path

# ... (other imports)

class TestAuditReportTableExtractor(unittest.TestCase):

    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True)
    @patch('src.audit_report_table_extractor.os.makedirs') # Mock makedirs
    @patch('src.audit_report_table_extractor.os.path.exists') # Mock path.exists
    def test_parse_audit_report_html_parsing(self, mock_path_exists, mock_makedirs, mock_to_csv, mock_requests_get):
        """Test successful HTML parsing, DataFrame creation, and metadata addition."""
        mock_path_exists.return_value = True # Assume output directory exists

        sample_html = """
        <html><body>
            <td class="c systemtitle3">Test Report Type Value</td>
            <td class="r systemtitle4">Jan 1, 2024</td>
            <table class="table">
                <tr><th>Col1</th><th>Col2</th></tr>
                <tr><td>R1C1</td><td>R1C2</td></tr>
                <tr><td>R2C1</td><td>R2C2</td></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock() # Mock to avoid actual HTTP error checking
        mock_requests_get.return_value = mock_response

        parse_audit_report("http://fakeurl.com/report.html", "test_report_name")

        # Expected path
        expected_path = TestPath("output") / "test_report_name.csv"
        
        # Check that to_csv was called correctly
        mock_to_csv.assert_called_once()
        
        # Retrieve the arguments from the call
        called_args = mock_to_csv.call_args
        df_passed_to_csv = called_args.args[0]  # The DataFrame instance (self)
        called_path = called_args.args[1]     # The path argument
        called_index_kwarg = called_args.kwargs.get('index')

        self.assertEqual(called_path, expected_path)
        self.assertFalse(called_index_kwarg, "index should be False")
        
        self.assertIsInstance(df_passed_to_csv, pd.DataFrame)
        self.assertIn("Col1", df_passed_to_csv.columns)
        self.assertIn("Col2", df_passed_to_csv.columns)
        self.assertIn("Report Type", df_passed_to_csv.columns)
        self.assertIn("Date Published", df_passed_to_csv.columns)

        self.assertEqual(df_passed_to_csv.shape[0], 2) # Number of data rows
        self.assertEqual(df_passed_to_csv.iloc[0]["Col1"], "R1C1")
        self.assertEqual(df_passed_to_csv.iloc[0]["Report Type"], "Test Report Type Value")
        self.assertEqual(df_passed_to_csv.iloc[0]["Date Published"], "Jan 1, 2024")


    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True) # Mock to_csv
    def test_parse_audit_report_request_error(self, mock_to_csv, mock_requests_get):
        """Test that requests.exceptions.RequestException is handled."""
        mock_requests_get.side_effect = requests.exceptions.RequestException("Network Error")

        # Expect the function to log an error and not call to_csv, and not crash
        parse_audit_report("http://fakeurl.com/report.html", "test_report_name_req_error")
        
        mock_requests_get.assert_called_once_with("http://fakeurl.com/report.html")
        mock_to_csv.assert_not_called()

    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True) # Mock to_csv
    def test_parse_audit_report_missing_table(self, mock_to_csv, mock_requests_get):
        """Test behavior when the main data table (class='table') is missing."""
        sample_html_no_table = """
        <html><body>
            <td class="c systemtitle3">Test Report Type Value</td>
            <td class="r systemtitle4">Jan 1, 2024</td>
            <p>No table here</p>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html_no_table.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        parse_audit_report("http://fakeurl.com/no_table.html", "test_report_no_table")
        
        # Should log a warning, not call to_csv, and not crash
        mock_to_csv.assert_not_called()

    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True) # Mock to_csv
    @patch('src.audit_report_table_extractor.os.makedirs')
    @patch('src.audit_report_table_extractor.os.path.exists')
    def test_parse_audit_report_missing_metadata(self, mock_path_exists, mock_makedirs, mock_to_csv, mock_requests_get):
        """Test behavior when metadata elements (Report Type, Date Published) are missing."""
        mock_path_exists.return_value = True

        sample_html_no_metadata = """
        <html><body>
            <table class="table">
                <tr><th>Col1</th><th>Col2</th></tr>
                <tr><td>R1C1</td><td>R1C2</td></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html_no_metadata.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        parse_audit_report("http://fakeurl.com/no_metadata.html", "test_report_no_metadata")
        
        expected_path = TestPath("output") / "test_report_no_metadata.csv"
        mock_to_csv.assert_called_once()

        called_args = mock_to_csv.call_args
        df_passed_to_csv = called_args.args[0]  # The DataFrame instance
        called_path = called_args.args[1]     # The path argument
        called_index_kwarg = called_args.kwargs.get('index')

        self.assertEqual(called_path, expected_path)
        self.assertFalse(called_index_kwarg, "index should be False")

        self.assertEqual(df_passed_to_csv.iloc[0]["Report Type"], "Unknown Report Type")
        self.assertEqual(df_passed_to_csv.iloc[0]["Date Published"], "Unknown Date")

    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True) # Mock to_csv
    @patch('src.audit_report_table_extractor.os.makedirs')
    @patch('src.audit_report_table_extractor.os.path.exists')
    def test_parse_audit_report_empty_table(self, mock_path_exists, mock_makedirs, mock_to_csv, mock_requests_get):
        """Test behavior with an empty data table (only headers)."""
        mock_path_exists.return_value = True
        sample_html_empty_table = """
        <html><body>
            <td class="c systemtitle3">Test Report Type</td>
            <td class="r systemtitle4">Jan 1, 2024</td>
            <table class="table">
                <tr><th>Col1</th><th>Col2</th></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html_empty_table.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response

        parse_audit_report("http://fakeurl.com/empty_table.html", "test_report_empty_table")
        
        # The function should log a warning and not call to_csv if no data rows are found.
        mock_to_csv.assert_not_called()


    @patch('src.audit_report_table_extractor.requests.get')
    @patch('pandas.DataFrame.to_csv', autospec=True)
    @patch('pathlib.Path.mkdir')  # Patching Path.mkdir directly
    @patch('src.audit_report_table_extractor.os.path.exists') # This mock might not be needed if Path.mkdir is the primary target for OSError
    def test_parse_audit_report_os_error_saving_csv(self, mock_os_path_exists, mock_path_mkdir, mock_to_csv, mock_requests_get):
        """Test behavior when Path.mkdir or df.to_csv raises an OSError."""
        # Scenario 1: Path.mkdir raises OSError
        mock_path_mkdir.side_effect = OSError("Failed to create directory")
        # os.path.exists might be checked before Path.mkdir depending on implementation details of Path.mkdir(exist_ok=True)
        # However, the direct error source we want to simulate is Path.mkdir itself.
        # Let's assume mock_os_path_exists can be True or False, mkdir should still be called.
        # For Path.mkdir(exist_ok=True), it first checks existence, then tries to make dirs.
        # To ensure our mock_path_mkdir is the one raising OSError, we can make mock_os_path_exists return False.
        mock_os_path_exists.return_value = False

        sample_html = """
        <html><body>
            <td class="c systemtitle3">Test Report Type</td>
            <td class="r systemtitle4">Jan 1, 2024</td>
            <table class="table">
                <tr><th>Col1</th><th>Col2</th></tr>
                <tr><td>R1C1</td><td>R1C2</td></tr>
            </table>
        </body></html>
        """
        mock_response = MagicMock()
        mock_response.content = sample_html.encode('utf-8')
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_requests_get.return_value = mock_response
        
        # Test OSError during Path.mkdir
        with self.assertLogs(level='ERROR') as log:
            parse_audit_report("http://fakeurl.com/os_error_dir.html", "test_os_error_dir")

        self.assertTrue(
            any("OS error during parsing or saving" in str(msg) and "Failed to create directory" in str(msg) for msg in log.output),
            f"Expected log message not found. Logs: {log.output}"
        )
        mock_to_csv.assert_not_called() # to_csv should not be called if directory creation fails

        # Reset mocks for next scenario: df.to_csv raises OSError
        mock_path_mkdir.side_effect = None
        mock_os_path_exists.return_value = True # Assume directory exists or is created successfully

        # Ensure previous mock_to_csv is reset if it was a different instance or clear its side_effect
        # If mock_to_csv is the same object, just reassign side_effect
        mock_to_csv.reset_mock() # Reset call count etc.
        mock_to_csv.side_effect = OSError("Failed to write CSV")
        with self.assertLogs(level='ERROR') as log:
            parse_audit_report("http://fakeurl.com/os_error_csv.html", "test_os_error_csv")
        self.assertTrue(
            any("OS error during parsing or saving" in str(msg) and "Failed to write CSV" in str(msg) for msg in log.output),
            f"Expected log message for to_csv OSError not found. Logs: {log.output}"
        )


if __name__ == '__main__':
    unittest.main()
