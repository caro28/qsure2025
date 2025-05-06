import unittest
from unittest import mock
from unittest.mock import patch, mock_open
import pandas as pd
import tempfile
import os
import shutil

import pytest

from src.filter_op import (
    get_ref_drug_names,
    get_op_raw_path,
    get_op_drug_columns,
    find_matches_op,
    filter_open_payments
)


def test_get_ref_drug_names(tmp_path):
    # Create test dataframe
    test_data = {
        'Brand_name_1': ['KEYTRUDA', 'XTANDI', None],
        'Brand_name_2': ['LYNPARZA', None, 'KEYTRUDA'],
        'Generic_name': ['pembrolizumab', 'enzalutamide', 'pembrolizumab IV']
    }
    pd.DataFrame(test_data).to_csv(tmp_path / "ProstateDrugList.csv", index=False)

    # Run function
    drug_names = get_ref_drug_names(tmp_path / "ProstateDrugList.csv")

    # Check expected results
    expected_names = {
        'keytruda',
        'xtandi',
        'lynparza',
        'pembrolizumab',
        'enzalutamide'
    }
    assert set(drug_names) == expected_names


class TestGetOpRawPath():
    def test_general_payments(self, tmp_path):
        year = 2022
        dataset_type = "general"
        expected_path = "data/raw/general_payments/OP_DTL_GNRL_PGYR2022_P01302025_01212025.csv"
        assert get_op_raw_path(year, dataset_type) == expected_path

    def test_research_payments(self, tmp_path):
        year = 2014
        dataset_type = "research"
        expected_path = "data/raw/research_payments/OP_DTL_RSRCH_PGYR2014_P01212022.csv"
        assert get_op_raw_path(year, dataset_type) == expected_path

    def test_file_not_found(self):
        year = 2012
        dataset_type = "general"    
        # Check that ValueError is raised when no file is found
        with pytest.raises(ValueError):
            get_op_raw_path(year, dataset_type)


class TestGetOpDrugColumns():
    def test_op_drug_cols_2014_2015(self):
        op_df = pd.DataFrame(columns=[
            "Name_of_Associated_Covered_Drug_or_Biological1",
            "Name_of_Associated_Covered_Drug_or_Biological100",
            "Name_of_Associated_Covered_Device_or_Medical_Supply1",
            "Name_of_Associated_Covered_Device_or_Medical_Supply100",
            "Other_Column",
            "Name_of_Associated_"
        ])
        year = 2014
        drug_cols = get_op_drug_columns(op_df, year)
        expected_cols = [
            "Name_of_Associated_Covered_Drug_or_Biological1",
            "Name_of_Associated_Covered_Drug_or_Biological100",
            "Name_of_Associated_Covered_Device_or_Medical_Supply1",
            "Name_of_Associated_Covered_Device_or_Medical_Supply100"
        ]
        assert set(drug_cols) == set(expected_cols)
    
    def test_op_drug_cols_2016_2023(self):
        op_df = pd.DataFrame(columns=[
            "name_of_drug_or_biological_or_device_or_medical_supply_1",
            "name_of_drug_or_biological_or_device_or_medical_supply_100",
            "name_of_device_or_medical_supply_1",
            "name_of_device_or_medical_supply_100",
            "Other_Column",
            "name_of_drug_or_biolo",
        ])
        year = 2016
        drug_cols = get_op_drug_columns(op_df, year)
        expected_cols = [
            "name_of_drug_or_biological_or_device_or_medical_supply_1",
            "name_of_drug_or_biological_or_device_or_medical_supply_100",
        ]
        assert set(drug_cols) == set(expected_cols)


class TestFindMatchesOp():
    def test_find_matches_op_matched(self):
        # mock op df
        op_chunk = pd.DataFrame(
            {
                "name_of_drug_or_biological_or_device_or_medical_supply_1": ["Lynparza", "drug1", "EnzalutAmide", "drug2"],
                "name_of_drug_or_biological_or_device_or_medical_supply_2": ["drug3", "tylenol", "jevtana", "drug4"],
            }
        )
        drug_cols = [
            "name_of_drug_or_biological_or_device_or_medical_supply_1",
            "name_of_drug_or_biological_or_device_or_medical_supply_2"
        ]
        ref_drug_names = ["lynparza", "enzalutamide", "jevtana"]
        
        filtered_chunk = find_matches_op(op_chunk, drug_cols, ref_drug_names)
        
        expected_chunk = pd.DataFrame(
            {
                "name_of_drug_or_biological_or_device_or_medical_supply_1": ["Lynparza", "EnzalutAmide"],
                "name_of_drug_or_biological_or_device_or_medical_supply_2": ["drug3", "jevtana"],
            },
            index=[0,2] # TODO: check it's ok that find_matches_op behaves this way
        )
        assert filtered_chunk.equals(expected_chunk)

    def test_find_matches_op_no_matches(self):
        pass


# class TestFilterOpenPayments(unittest.TestCase):
#     def setUp(self):
#         # Sample test data with lowercase names (cleaned format)
#         self.test_drugs = pd.DataFrame({
#             'brand_name': ['lynparza', 'keytruda'],
#             'generic_name': ['enzalutamide', 'pembrolizumab']
#         })
        
#         # Sample OpenPayments data
#         self.test_op_data = pd.DataFrame({
#             'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1': ['Lynparza', 'Keytruda', 'Enzalutamide', 'Pembrolizumab'],
#             'payment_amount': [100, 200, 300, 400]
#         })

#     @patch('src.filter_op.get_ref_drug_names')
#     @patch('src.filter_op.get_op_raw_path')
#     @patch('src.filter_op.pd.read_csv')
#     @patch('builtins.open', new_callable=mock_open)
#     def test_filter_open_payments_matches(self, mock_file, mock_read_csv, mock_get_path, mock_get_drugs):
#         # Setup mocks with lowercase drug names
#         mock_get_drugs.return_value = set(['lynparza', 'keytruda', 'enzalutamide', 'pembrolizumab'])
#         mock_get_path.return_value = 'dummy/path'
#         # Mock read_csv to return a DataFrame for the nrows=1 call
#         # and an iterator of DataFrames for the chunksize calls
#         def read_csv_side_effect(*args, **kwargs):
#             if kwargs.get('nrows') == 1:
#                 return self.test_op_data.head(1)
#             elif kwargs.get('chunksize'):
#                 return iter([self.test_op_data])
#             else:
#                 return self.test_op_data

#         mock_read_csv.side_effect = read_csv_side_effect

#         # Run function
#         filter_open_payments(2020, 'General')

#         # Assert that the correct number of matches were found
#         # In this case, we expect 3 matches (Lynparza, Keytruda, Enzalutamide)
#         self.assertEqual(mock_file.call_count, 1)  # One file should be written
        
#     @patch('src.filter_op.get_ref_drug_names')
#     @patch('src.filter_op.get_op_raw_path')
#     @patch('src.filter_op.pd.read_csv')
#     @patch('builtins.open', new_callable=mock_open)
#     def test_filter_open_payments_no_matches(self, mock_file, mock_read_csv, mock_get_path, mock_get_drugs):
#         # Setup mocks with no matching drugs
#         mock_get_drugs.return_value = set(['UnrelatedDrug1', 'UnrelatedDrug2'])
#         mock_get_path.return_value = 'dummy/path'
#         def read_csv_side_effect(*args, **kwargs):
#             if kwargs.get('nrows') == 1:
#                 return self.test_op_data.head(1)
#             elif kwargs.get('chunksize'):
#                 return iter([self.test_op_data])
#             else:
#                 return self.test_op_data

#         # Run function
#         filter_open_payments(2020, 'General')

#         # Assert that no files were written since there were no matches
#         self.assertEqual(mock_file.call_count, 0)



if __name__ == '__main__':
    unittest.main()
