import unittest
from unittest import mock
from unittest.mock import patch, mock_open
import pandas as pd
import tempfile
import os
import shutil

from src.filter_op import (
    get_ref_drug_names,
    get_op_raw_path,
    get_op_drug_columns,
    filter_open_payments
)


class TestGetRefDrugNames(unittest.TestCase):
    def setUp(self):
        # Create a temporary CSV file with test data
        self.temp_dir = tempfile.mkdtemp()
        self.test_csv = os.path.join(self.temp_dir, "ProstateDrugList.csv")
        
        # Create test dataframe
        test_data = {
            'Brand_name_1': ['KEYTRUDA', 'XTANDI', None],
            'Brand_name_2': ['LYNPARZA', None, 'KEYTRUDA'],  # Duplicate KEYTRUDA
            'Generic_name': ['pembrolizumab', 'enzalutamide', 'pembrolizumab IV']
        }
        pd.DataFrame(test_data).to_csv(self.test_csv, index=False)
        
        # Mock pd.read_csv to return our test dataframe
        self.patcher = mock.patch('pandas.read_csv')
        self.mock_read_csv = self.patcher.start()
        self.mock_read_csv.return_value = pd.DataFrame(test_data)

    def tearDown(self):
        # Clean up the temporary directory and file
        shutil.rmtree(self.temp_dir)
        self.patcher.stop()

    def test_get_ref_drug_names(self):
        drug_names = get_ref_drug_names()
        
        # Check expected results
        expected_names = {
            'keytruda',          # from Brand_name columns
            'xtandi',
            'lynparza',
            'pembrolizumab',     # from Generic_name column
            'enzalutamide'
        }
        
        self.assertEqual(set(drug_names), expected_names)
        
        # Check that duplicates were removed
        self.assertEqual(len(drug_names), len(set(drug_names)))
        
        # Check that None and empty values were removed
        self.assertNotIn('', drug_names)
        self.assertNotIn(None, drug_names)


class TestGetOpRawPath(unittest.TestCase):
    def test_general_payments(self):
        # Mock os.listdir to return a list with our test filename
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['OP_DTL_GNRL_PGYR2022_P06302023.csv']
            
            # Test general payments path
            path = get_op_raw_path(2022, "general")
            
            # Check correct path is returned
            expected = os.path.join("data/raw/general_payments/", 
                                  "OP_DTL_GNRL_PGYR2022_P06302023.csv")
            self.assertEqual(path, expected)

    def test_research_payments(self):
        # Mock os.listdir to return a list with our test filename
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = ['OP_DTL_RSRCH_PGYR2022_P06302023.csv']
            
            # Test research payments path
            path = get_op_raw_path(2022, "research")
            
            # Check correct path is returned
            expected = os.path.join("data/raw/research_payments/", 
                                  "OP_DTL_RSRCH_PGYR2022_P06302023.csv")
            self.assertEqual(path, expected)

    def test_file_not_found(self):
        # Mock os.listdir to return an empty list
        with mock.patch('os.listdir') as mock_listdir:
            mock_listdir.return_value = []
            
            # Check that ValueError is raised when no file is found
            with self.assertRaises(ValueError):
                get_op_raw_path(2022, "general")


class TestGetOpDrugColumns(unittest.TestCase):
    def test_get_drug_columns(self):
        # Create test dataframe with mix of drug and non-drug columns
        test_df = pd.DataFrame(columns=[
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',  # Should match
            'name_of_drug_or_biological_or_device_or_medical_supply_2',  # Should match
            'Record_ID',                                                 # Should not match
            'Physician_Name',                                           # Should not match
            'NAME_OF_DRUG_OR_BIOLOGICAL_OR_DEVICE_OR_MEDICAL_SUPPLY_3'  # Should match
        ])
        
        # Get drug columns
        drug_cols = get_op_drug_columns(test_df)
        
        # Check results
        expected_cols = [
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
            'name_of_drug_or_biological_or_device_or_medical_supply_2',
            'NAME_OF_DRUG_OR_BIOLOGICAL_OR_DEVICE_OR_MEDICAL_SUPPLY_3'
        ]
        
        # Check that we found all drug columns regardless of case
        self.assertEqual(set(drug_cols), set(expected_cols))
        
        # Check that we found exactly the right number of columns
        self.assertEqual(len(drug_cols), 3)


class TestFilterOpenPayments(unittest.TestCase):
    def setUp(self):
        # Sample test data with lowercase names (cleaned format)
        self.test_drugs = pd.DataFrame({
            'brand_name': ['lynparza', 'keytruda'],
            'generic_name': ['enzalutamide', 'pembrolizumab']
        })
        
        # Sample OpenPayments data
        self.test_op_data = pd.DataFrame({
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1': ['Lynparza', 'Keytruda', 'Enzalutamide', 'Pembrolizumab'],
            'payment_amount': [100, 200, 300, 400]
        })

    @patch('src.filter_op.get_ref_drug_names')
    @patch('src.filter_op.get_op_raw_path')
    @patch('src.filter_op.pd.read_csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_filter_open_payments_matches(self, mock_file, mock_read_csv, mock_get_path, mock_get_drugs):
        # Setup mocks with lowercase drug names
        mock_get_drugs.return_value = set(['lynparza', 'keytruda', 'enzalutamide', 'pembrolizumab'])
        mock_get_path.return_value = 'dummy/path'
        # Mock read_csv to return a DataFrame for the nrows=1 call
        # and an iterator of DataFrames for the chunksize calls
        def read_csv_side_effect(*args, **kwargs):
            if kwargs.get('nrows') == 1:
                return self.test_op_data.head(1)
            elif kwargs.get('chunksize'):
                return iter([self.test_op_data])
            else:
                return self.test_op_data

        mock_read_csv.side_effect = read_csv_side_effect

        # Run function
        filter_open_payments(2020, 'General')

        # Assert that the correct number of matches were found
        # In this case, we expect 3 matches (Lynparza, Keytruda, Enzalutamide)
        self.assertEqual(mock_file.call_count, 1)  # One file should be written
        
    @patch('src.filter_op.get_ref_drug_names')
    @patch('src.filter_op.get_op_raw_path')
    @patch('src.filter_op.pd.read_csv')
    @patch('builtins.open', new_callable=mock_open)
    def test_filter_open_payments_no_matches(self, mock_file, mock_read_csv, mock_get_path, mock_get_drugs):
        # Setup mocks with no matching drugs
        mock_get_drugs.return_value = set(['UnrelatedDrug1', 'UnrelatedDrug2'])
        mock_get_path.return_value = 'dummy/path'
        def read_csv_side_effect(*args, **kwargs):
            if kwargs.get('nrows') == 1:
                return self.test_op_data.head(1)
            elif kwargs.get('chunksize'):
                return iter([self.test_op_data])
            else:
                return self.test_op_data

        # Run function
        filter_open_payments(2020, 'General')

        # Assert that no files were written since there were no matches
        self.assertEqual(mock_file.call_count, 0)



if __name__ == '__main__':
    unittest.main()
