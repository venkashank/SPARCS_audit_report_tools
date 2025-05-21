import sys
import os
import unittest
from unittest.mock import patch, MagicMock
import pandas as pd
import numpy as np

# Add src directory to sys.path to allow direct import of modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.compliance_table_extractor import process_pdf

class TestComplianceTableExtractor(unittest.TestCase):

    def create_mock_camelot_table(self, df_data):
        """Helper to create a mock Camelot table object with a DataFrame."""
        mock_table = MagicMock()
        # Camelot's table.df returns a DataFrame
        mock_table.df = pd.DataFrame(df_data)
        return mock_table

    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_data_transformation(self, mock_read_pdf):
        """Test the core data transformation logic of process_pdf."""
        sample_df_data = {
            0: ['File\nType', 'Type A', 'Total Records Submitted', 'Type B', 'Type B'], # First row is header
            1: ['Some Column', 'Data1', 'Data2', 'Data3', 'Data4'],
            2: ['PCT_OF_PREVYRAVG_SUBMTD_', '50%', 'N/A', '75.5%', '20%']
        }
        
        # Camelot returns a list of Table objects. Each Table has a .df attribute.
        mock_camelot_table_obj = self.create_mock_camelot_table(sample_df_data)
        mock_read_pdf.return_value = [mock_camelot_table_obj] # Simulate one table found

        # Example filename: Y<year>_some_other_parts_<pfi>.pdf
        test_pdf_filename = "pdfs/Y2023_AUDIT_REPORT_PFI123.pdf"
        result_dfs = process_pdf(test_pdf_filename)

        self.assertEqual(len(result_dfs), 1, "Should return one processed DataFrame")
        processed_df = result_dfs[0]

        # Test column renaming and case conversion
        self.assertIn("FILE_TYPE", processed_df.columns)
        self.assertIn("SOME_COLUMN", processed_df.columns)
        self.assertIn("PCT_OF_PREVYRAVG_SUBMTD_", processed_df.columns)

        # Test filtering of "Total Records Submitted"
        self.assertFalse((processed_df["FILE_TYPE"] == "Total Records Submitted").any())
        self.assertFalse((processed_df["FILE_TYPE"] == "File\nType").any())


        # Test ffill on FILE_TYPE
        self.assertEqual(processed_df["FILE_TYPE"].tolist(), ['Type A', 'Type B', 'Type B'])
        
        # Test percentage conversion
        # Original data: 'Type A', 'Type B', 'Type B' corresponds to '50%', '75.5%', '20%'
        expected_percentages = [0.50, 0.755, 0.20]
        self.assertTrue(np.allclose(processed_df["PCT_OF_PREVYRAVG_SUBMTD_"].tolist(), expected_percentages))

        # Test addition of PFI and AUDIT_YEAR
        self.assertTrue((processed_df["PFI"] == "PFI123").all())
        self.assertTrue((processed_df["AUDIT_YEAR"] == "2023").all())
        
        mock_read_pdf.assert_called_once_with(test_pdf_filename, pages="1-end", suppress_stdout=True, line_scale=40)

    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_camelot_error(self, mock_read_pdf):
        """Test that process_pdf returns an empty list if camelot.read_pdf raises an exception."""
        mock_read_pdf.side_effect = Exception("Camelot failed!")
        result_dfs = process_pdf("pdfs/Y2023_ERROR_PFI_Error.pdf")
        self.assertEqual(len(result_dfs), 0)

    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_no_tables_found(self, mock_read_pdf):
        """Test that process_pdf returns an empty list if no tables are found by camelot."""
        mock_read_pdf.return_value = [] # Simulate camelot finding no tables
        result_dfs = process_pdf("pdfs/Y2023_NO_TABLES_PFI_NoTable.pdf")
        self.assertEqual(len(result_dfs), 0)

    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_table_without_df(self, mock_read_pdf):
        """Test that process_pdf handles tables that might not have a 'df' attribute correctly."""
        mock_table_no_df = MagicMock()
        del mock_table_no_df.df # Ensure df attribute is missing
        mock_read_pdf.return_value = [mock_table_no_df]
        
        # This scenario should ideally be caught by an AttributeError or similar
        # The current implementation of process_pdf might raise an error if table.df is not present.
        # Let's assume robust error handling within the loop for tables.
        # For now, we expect it to skip the problematic table and return an empty list if it's the only one.
        # Depending on actual error handling in process_pdf, this test might need adjustment.
        # Update: The code has try-except around table processing.
        result_dfs = process_pdf("pdfs/Y2023_TABLE_NO_DF_PFI_NoDF.pdf")
        self.assertEqual(len(result_dfs), 0, "Should return an empty list if tables lack 'df' or cause errors")


    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_filename_parsing(self, mock_read_pdf):
        """Test PFI and AUDIT_YEAR extraction from different filename formats."""
        mock_read_pdf.return_value = [self.create_mock_camelot_table({0: ['FILE_TYPE', 'Data'], 1: ['Value', '1']})]

        # Test standard filename
        result_dfs_standard = process_pdf("pdfs/Y2022_AUDIT_PFI999.pdf")
        self.assertEqual(result_dfs_standard[0]["AUDIT_YEAR"].iloc[0], "2022")
        self.assertEqual(result_dfs_standard[0]["PFI"].iloc[0], "PFI999")

        # Test filename with different parts
        result_dfs_variant = process_pdf("pdfs/Y2021_XYZ_REPORT_PFIABC.pdf")
        self.assertEqual(result_dfs_variant[0]["AUDIT_YEAR"].iloc[0], "2021")
        self.assertEqual(result_dfs_variant[0]["PFI"].iloc[0], "PFIABC")
        
        # Test filename where year might not be Y prefixed (current logic expects 'Y')
        # The current logic for year extraction is `name_parts[0][1:]` if it starts with 'Y'
        # If it doesn't start with 'Y', it logs a warning and sets 'UnknownYear'.
        result_dfs_no_y = process_pdf("pdfs/2020_REPORT_PFIXYZ.pdf")
        self.assertEqual(result_dfs_no_y[0]["AUDIT_YEAR"].iloc[0], "UnknownYear")
        self.assertEqual(result_dfs_no_y[0]["PFI"].iloc[0], "PFIXYZ")


    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_empty_df_from_camelot(self, mock_read_pdf):
        """Test handling of an empty DataFrame returned by Camelot for a table."""
        mock_read_pdf.return_value = [self.create_mock_camelot_table({})] # Empty DataFrame
        
        result_dfs = process_pdf("pdfs/Y2023_EMPTY_DF_PFI_Empty.pdf")
        # The current code might raise an error if df.iloc[0] is accessed on an empty df.
        # Good error handling should catch this and skip the table.
        # Update: The code has try-except around table processing.
        self.assertEqual(len(result_dfs), 0, "Should skip tables that result in empty or problematic DataFrames")

    @patch('src.compliance_table_extractor.camelot.read_pdf')
    def test_process_pdf_missing_pct_column(self, mock_read_pdf):
        """Test behavior when 'PCT_OF_PREVYRAVG_SUBMTD_' column is missing."""
        sample_df_data = {
            0: ['FILE_TYPE', 'Type A'], # Header
            1: ['Some Column', 'Data1']
            # PCT_OF_PREVYRAVG_SUBMTD_ column is missing
        }
        mock_read_pdf.return_value = [self.create_mock_camelot_table(sample_df_data)]
        
        result_dfs = process_pdf("pdfs/Y2023_MISSING_COL_PFI_NoPct.pdf")
        self.assertEqual(len(result_dfs), 1)
        processed_df = result_dfs[0]
        self.assertIn("PCT_OF_PREVYRAVG_SUBMTD_", processed_df.columns) # Column should be added
        self.assertTrue(processed_df["PCT_OF_PREVYRAVG_SUBMTD_"].isna().all()) # All values should be NaN

if __name__ == '__main__':
    unittest.main()
