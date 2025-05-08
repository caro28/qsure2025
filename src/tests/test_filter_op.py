import pandas as pd
import os

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
            index=[0,2]
        )
        assert filtered_chunk.equals(expected_chunk)

    def test_find_matches_op_no_matches(self):
        # mock op df
        op_chunk = pd.DataFrame(
            {
                "name_of_drug_or_biological_or_device_or_medical_supply_1": ["tylenol", "drug1", "adVIL", "tUms"],
                "name_of_drug_or_biological_or_device_or_medical_supply_2": ["drug3", "tylenol", "drug4", "drug5"],
            }
        )
        drug_cols = [
            "name_of_drug_or_biological_or_device_or_medical_supply_1",
            "name_of_drug_or_biological_or_device_or_medical_supply_2"
        ]
        ref_drug_names = ["lynparza", "enzalutamide", "jevtana"]
        filtered_chunk = find_matches_op(op_chunk, drug_cols, ref_drug_names)
        expected_chunk = pd.DataFrame(
            columns=drug_cols
        )
        assert filtered_chunk.empty
        assert filtered_chunk.equals(expected_chunk)


class TestFilterOpenPayments():
    def test_filter_open_payments_2016_2023(self, tmp_path):
        year = 2022
        dataset_type = "general"
        ref_path = "data/reference/ProstateDrugList.csv"

        test_data = {
        "name_of_drug_or_biological_or_device_or_medical_supply_1": [
            "Lynparza", "drug1", "EnzalutAmide", "drug2"
            ],
        "name_of_drug_or_biological_or_device_or_medical_supply_2": [
            "drug3", "tylenol", "jevtana", "drug4"
            ],
        "other_column": [
            "other0", "other1", "other2", "other3"
            ]
        }
        pd.DataFrame(test_data).to_csv(tmp_path / "test_filter_op_file.csv", index=False)
        op_path = tmp_path / "test_filter_op_file.csv"
        dir_out = tmp_path / "test_filter_op_chunks"
        dir_out.mkdir()

        # run function (add "/" to end of dir_out because function expects dir_out to end with "/")
        filter_open_payments(year, dataset_type, ref_path, op_path, f"{dir_out}/")
        
        # index=False when writing to csv, so index is reset (index != [0,2])
        expected_chunk = pd.DataFrame(
            {
                "name_of_drug_or_biological_or_device_or_medical_supply_1": ["Lynparza", "EnzalutAmide"],
                "name_of_drug_or_biological_or_device_or_medical_supply_2": ["drug3", "jevtana"],
                "other_column": ["other0", "other2"]
            }
        )

        # Check that the filtered chunks were created
        assert os.path.exists(dir_out / "general_2022_chunk_0.csv")
        filtered_chunk = pd.read_csv(dir_out / "general_2022_chunk_0.csv")
        assert filtered_chunk.equals(expected_chunk)


