import unittest
import pandas as pd
import json
import os
from io import StringIO
from unittest.mock import patch, mock_open, MagicMock
from .._utils import get_drug_data, clean_brand_name, clean_generic_name

class TestDrugDataProcessing(unittest.TestCase):
    def test_clean_brand_name(self):
        """Test brand name cleaning with various inputs"""
        test_cases = [
            # Basic cleaning
            ("Zytiga", "zytiga"),
            # Whitespace handling
            ("  Xtandi  ", "xtandi"),
            # Punctuation removal
            ("Zytiga® (250mg)", "zytiga 250mg"),
            # Special characters
            ("XTANDI™", "xtandi"),
            # Slashes and hyphens
            ("Casodex/Bicalutamide", "casodexbicalutamide"),
            ("Erleada-60", "erleada60"),
            # Non-ASCII characters
            ("Médication", "medication"),
            # Non-string input
            (None, ""),
            (123, "")
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = clean_brand_name(input_name)
                self.assertEqual(result, expected)

    def test_clean_generic_name(self):
        """Test generic name cleaning with various inputs"""
        test_cases = [
            # Basic cleaning
            ("Abiraterone", "abiraterone"),
            # Whitespace handling
            ("  Enzalutamide  ", "enzalutamide"),
            # Administration route removal
            ("Abiraterone PO", "abiraterone"),
            ("Enzalutamide IV", "enzalutamide"),
            ("Medication IM", "medication"),
            ("Treatment SUBQ", "treatment"),
            # Case insensitive route removal
            ("Medicine po", "medicine"),
            ("Drug iv", "drug"),
            # Multiple routes (should only remove at end)
            ("IV Medication PO", "iv medication"),
            # Punctuation and routes
            ("Med-Name (IV)", "medname"),
            # Non-ASCII characters
            ("Généric PO", "generic"),
            # Non-string input
            (None, ""),
            (123, "")
        ]
        
        for input_name, expected in test_cases:
            with self.subTest(input_name=input_name):
                result = clean_generic_name(input_name)
                self.assertEqual(result, expected)

    def test_get_drug_data(self):
        """Test drug data extraction from CSV"""
        # Mock CSV content
        csv_content = """Brand_name1,Brand_name2,Generic_name,Yellow_Green
Zytiga®,,Abiraterone PO,yellow
XTANDI™,Enza,Enzalutamide IV,green
Casodex,Bicalutamide,Bicalutamide,"""
        
        # Create mock DataFrame
        mock_df = pd.read_csv(StringIO(csv_content))
        
        # Mock pd.read_csv to return our test DataFrame
        with patch('pandas.read_csv', return_value=mock_df):
            result = get_drug_data("dummy_path.csv")
            
            # Check first drug
            self.assertIn("zytiga", result)
            self.assertEqual(result["zytiga"]["generic"], "abiraterone")
            self.assertEqual(result["zytiga"]["drug_type_color"], "yellow")
            
            # Check second drug (both brand names)
            self.assertIn("xtandi", result)
            self.assertEqual(result["xtandi"]["generic"], "enzalutamide")
            self.assertEqual(result["xtandi"]["drug_type_color"], "green")
            
            self.assertIn("enza", result)
            self.assertEqual(result["enza"]["generic"], "enzalutamide")
            self.assertEqual(result["enza"]["drug_type_color"], "green")
            
            # Check third drug
            self.assertIn("casodex", result)
            self.assertEqual(result["casodex"]["generic"], "bicalutamide")
            self.assertEqual(result["casodex"]["drug_type_color"], "")

    def test_get_drug_data_empty_file(self):
        """Test handling of empty CSV file"""
        # Create empty DataFrame
        mock_df = pd.DataFrame(columns=['Brand_name1', 'Generic_name', 'Yellow_Green'])
        
        # Mock pd.read_csv to return empty DataFrame
        with patch('pandas.read_csv', return_value=mock_df):
            result = get_drug_data("dummy_path.csv")
            self.assertEqual(result, {})

    def test_get_drug_data_missing_columns(self):
        """Test handling of CSV with missing columns"""
        # Mock CSV with missing Yellow_Green column
        csv_content = """Brand_name1,Generic_name
Zytiga,Abiraterone
XTANDI,Enzalutamide"""
        
        mock_df = pd.read_csv(StringIO(csv_content))
        
        # Mock pd.read_csv to return DataFrame with missing column
        with patch('pandas.read_csv', return_value=mock_df):
            result = get_drug_data("dummy_path.csv")
            
            # Check that drugs are processed with empty drug_type_color
            self.assertIn("zytiga", result)
            self.assertEqual(result["zytiga"]["generic"], "abiraterone")
            self.assertEqual(result["zytiga"]["drug_type_color"], "")

    def test_get_drug_data_json_output(self):
        """Test that drug data is correctly saved to JSON file"""
        # Mock CSV content
        csv_content = """Brand_name1,Brand_name2,Generic_name,Yellow_Green
Zytiga®,,Abiraterone PO,yellow"""
        
        # Create mock DataFrame
        mock_df = pd.read_csv(StringIO(csv_content))
        
        # Create a temporary path for testing
        test_output = "test_output.json"
        
        # Mock os.makedirs to do nothing
        with patch('os.makedirs') as mock_makedirs:
            # Mock pd.read_csv to return our test DataFrame
            with patch('pandas.read_csv', return_value=mock_df):
                # Mock open() to capture written data
                mock_file = MagicMock()
                with patch('builtins.open', mock_open()) as mock_file:
                    result = get_drug_data("dummy_path.csv", test_output)
                    
                    # Verify that makedirs was called with the correct path
                    mock_makedirs.assert_called_once_with(os.path.dirname(test_output), exist_ok=True)
                    
                    # Verify that the file was opened for writing
                    mock_file.assert_called_once_with(test_output, 'w')
                    
                    # Get the data that would have been written to the file
                    written_data = mock_file().write.call_args[0][0]
                    saved_data = json.loads(written_data)
                    
                    # Verify the content matches the expected data
                    self.assertEqual(saved_data, result)
                    self.assertEqual(saved_data["zytiga"]["generic"], "abiraterone")
                    self.assertEqual(saved_data["zytiga"]["drug_type_color"], "yellow")

if __name__ == '__main__':
    unittest.main() 