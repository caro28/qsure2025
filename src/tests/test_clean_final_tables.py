import pandas as pd
from src.clean_final_tables import (
    add_new_columns,
    add_npis_2014, 
    build_map_year2cols, 
    build_ref_data_maps,
    clean_op_data,
    get_harmonized_drug_cols,
    get_prostate_drug_type, 
    harmonize_col_names,
    is_onc_prescriber,
    merge_cols_2014_2015,
    prep_general_data,
    prep_research_data
)


def test_build_map_year2cols(tmp_path):
    test_data = pd.DataFrame({
        '2014': ['col1', None, 'col2', 'col3'],
        '2015': ['col4', None, 'col5', 'col6'],
        '2016': ['col7', 'col8', None, 'col9']
    })
    test_data.to_csv(tmp_path / 'test_data.csv', index=False)
    
    path_to_test_cols = tmp_path / 'test_data.csv'
    year2cols = build_map_year2cols('general', path_to_test_cols)
    assert year2cols == {
        '2014': ['col1', 'col2', 'col3'],
        '2015': ['col4', 'col5', 'col6'],
        '2016': ['col7', 'col8', 'col9']
        }


def test_build_ref_data_maps(tmp_path):
    ref_data = pd.DataFrame(
        {
            'Generic_name': ['generic_a IV', 'generic_b'],
            'Brand_name1': ['DRUG_A', 'DRUG_B'],
            'Brand_name2': ['DociVyx', 'Eligard'],
            'Brand_name3': ['Docivyx', 'Eligard'],
            'Brand_name4': ['Docivyx', 'Eligard'],
            'Color': ['yellow', 'green']
        }
    )
    ref_data.to_csv(tmp_path / 'test_ref_data.csv', index=False)

    brand2generic, brand2color = build_ref_data_maps(tmp_path / 'test_ref_data.csv')
    assert brand2generic == {
        'druga': 'generica',
        'drugb': 'genericb',
        'docivyx': 'generica',
        'eligard': 'genericb',
        'generica': 'generica',
        'genericb': 'genericb'
    }
    assert brand2color == {
        'druga': 'yellow',
        'drugb': 'green',
        'generica': 'yellow',
        'genericb': 'green',
        'docivyx': 'yellow',
        'eligard': 'green'
    }


def test_harmonize_col_names(tmp_path):
    test_cols = pd.DataFrame({
        '2014': ['col1', None, 'col2', 'col3'],
        '2015': ['col4', None, 'col5', 'col6'],
        '2016': ['col7', 'col8', None, 'col9']
    })
    test_cols.to_csv(tmp_path / 'test_cols.csv', index=False)
    
    path_to_test_cols = tmp_path / 'test_cols.csv'

    # Create test input data
    test_df = pd.DataFrame({
        'colA': ['DRUG_A', 'DRUG_B', ''],
        'colB': ['123', '456', '789'],
        'colC': ['xyz', 'asd', 'fgh']
    })
    test_df.to_csv(tmp_path / 'test_data.csv', index=False)

    # Run function
    result = harmonize_col_names(test_df, '2014', 'general', path_to_test_cols)
    assert result.columns.to_list() == ['col1', 'col2', 'col3']


def test_get_harmonized_drug_cols():
    test_df = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['DRUG_A', 'DRUG_B', ''],
        'Drug_Biological_Device_Med_Sup_2': ['DRUG_C', 'DRUG_D', ''],
        'Drug_Biological_Device_Med_Sup_3': ['DRUG_E', 'DRUG_F', ''],
        'Covered_Recipient_NPI': ['123', '456', '789'],
        'Other_Col': ['123', '456', '789']
    })
    result = get_harmonized_drug_cols(test_df)
    assert result == [
        'Drug_Biological_Device_Med_Sup_1',
        'Drug_Biological_Device_Med_Sup_2',
        'Drug_Biological_Device_Med_Sup_3'
        ]


def test_get_prostate_drug_type():
    brand2color = {
        'DRUG_A': 'yellow',
        'DRUG_B': 'green',
        'generic_a': 'yellow',
        'generic_b': 'green'
    }
    result = get_prostate_drug_type('DRUG_A', brand2color)
    assert result == 1

    result = get_prostate_drug_type('DRUG_B', brand2color)
    assert result == 0
    
    result = get_prostate_drug_type('generic_a', brand2color)
    assert result == 1
    

def test_is_onc_prescriber():
    npi_set = {'123', '456'}
    prostate_drug_type = 1
    recipient_npis = ['123']
    result = is_onc_prescriber(prostate_drug_type, recipient_npis, npi_set)
    assert result == 1

    prostate_drug_type = 0
    recipient_npis = ['123', '456']
    result = is_onc_prescriber(prostate_drug_type, recipient_npis, npi_set)
    assert result == 0

    prostate_drug_type = 1
    recipient_npis = ['321', '654']
    result = is_onc_prescriber(prostate_drug_type, recipient_npis, npi_set)
    assert result == 0


def test_add_new_columns():
    test_df = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['DRUG_A', 'Radium 223'],
        'Drug_Biological_Device_Med_Sup_2': ['LynpA-rza', ''],
        'Covered_Recipient_NPI': ['123', '456'],
        'other_column': ['col0', 'col1']
    })
    drug_cols = [
        'Drug_Biological_Device_Med_Sup_1', 
        'Drug_Biological_Device_Med_Sup_2'
        ]
    npi_set = ['123']
    dataset_type = 'general'

    result = add_new_columns(test_df, drug_cols, npi_set, dataset_type)

    expected_result = pd.DataFrame(
        {
        'Drug_Biological_Device_Med_Sup_1': ['DRUG_A', 'Radium 223 IV'],
        'Drug_Biological_Device_Med_Sup_2': ['LynpA-rza', ''],
        'Covered_Recipient_NPI': [123, 456],
        'other_column': ['col0', 'col1'],
        'Drug_Name': ['olaparib', 'radium223'],
        'Prostate_Drug_Type': [1.0, 0.0], # decimals removed later in pipeline
        'Onc_Prescriber': [1.0, 0.0]
        }
    )

    assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
    assert set(result['Drug_Name'].values) == set(expected_result['Drug_Name'].values)
    assert set(result['Prostate_Drug_Type'].values) == set(expected_result['Prostate_Drug_Type'].values)
    assert set(result['Onc_Prescriber'].values) == set(expected_result['Onc_Prescriber'].values)


def test_prep_general_data(tmp_path):
    test_df = pd.DataFrame({
        'Covered_Recipient_NPI': ['123.0', pd.NA, '456.0', '789.0', pd.NA],
        'Other_Col': [0, 1, 2, 3, 4]
    })

    filename = "test_general_2017.csv"
    dir_missing_npis = tmp_path / "final_files/general_payments/missing_npis"
    dir_missing_npis.mkdir(parents=True, exist_ok=True)

    result = prep_general_data(test_df, filename, f"{dir_missing_npis}/")

    expected_result = pd.DataFrame({
        'Covered_Recipient_NPI': ['123', '456', '789'],
        'Other_Col': [0, 2, 3]
    })

    assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
    assert set(result['Covered_Recipient_NPI'].values) == set(expected_result['Covered_Recipient_NPI'].values)
    assert set(result['Other_Col'].values) == set(expected_result['Other_Col'].values)


def test_prep_research_data(tmp_path):
    test_df = pd.DataFrame({
        'Covered_Recipient_NPI': ['123', pd.NA, '456', '789', pd.NA],
        'PI_1_NPI': [pd.NA, '321.0', '654.0', '987.0', pd.NA],
        'PI_2_NPI': ['111.0', pd.NA, '222.0', '333', pd.NA],
        'Other_Col': [0, 1, 2, 3, 4]
    })

    filename = "test_research_2022.csv"
    dir_missing_npis = tmp_path / "final_files/research_payments/missing_npis"
    dir_missing_npis.mkdir(parents=True, exist_ok=True)

    result = prep_research_data(test_df, filename, f"{dir_missing_npis}/")

    expected_result = pd.DataFrame({
        'Covered_Recipient_NPI': ['123', pd.NA, '456', '789'],
        'PI_1_NPI': [pd.NA, '321', '654', '987'],
        'PI_2_NPI': ['111', pd.NA, '222', '333'],
        'Other_Col': [0, 1, 2, 3]
    })

    assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
    assert set(result['Covered_Recipient_NPI'].values) == set(expected_result['Covered_Recipient_NPI'].values)
    assert set(result['PI_1_NPI'].values) == set(expected_result['PI_1_NPI'].values)
    assert set(result['PI_2_NPI'].values) == set(expected_result['PI_2_NPI'].values)
    assert set(result['Other_Col'].values) == set(expected_result['Other_Col'].values)


def test_merge_cols_2014_2015():
    test_df = pd.DataFrame({
        'Name_of_Associated_Covered_Drug_or_Biological1': ['DRUG_A', '', 'DRUG_C'],
        'Name_of_Associated_Covered_Device_or_Medical_Supply1': ['', 'DRUG_B', 'DRUG_D'],
        'Name_of_Associated_Covered_Drug_or_Biological2': ['', 'DRUG_B', ''],
        'Name_of_Associated_Covered_Device_or_Medical_Supply2': ['DRUG_A', '', ''],
        'Name_of_Associated_Covered_Drug_or_Biological3': ['DRUG_A', 'DRUG_B', 'DRUG_C'],
        'Name_of_Associated_Covered_Device_or_Medical_Supply3': ['DRUG_X', 'DRUG_Y', 'DRUG_Z'],
        'Name_of_Associated_Covered_Drug_or_Biological4': ['', 'DRUG_A', ''],
        'Name_of_Associated_Covered_Device_or_Medical_Supply4': ['', '', ''],
        'Name_of_Associated_Covered_Drug_or_Biological5': ['', '', ''],
        'Name_of_Associated_Covered_Device_or_Medical_Supply5': ['', '', ''],
    })
    
    result = merge_cols_2014_2015(test_df)
    expected_result = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['DRUG_A', 'DRUG_B', 'DRUG_C'],
        'Drug_Biological_Device_Med_Sup_2': ['DRUG_A', 'DRUG_B', ''],
        'Drug_Biological_Device_Med_Sup_3': ['DRUG_A', 'DRUG_B', 'DRUG_C'],
        'Drug_Biological_Device_Med_Sup_4': ['DRUG_A', '', ''],
        'Drug_Biological_Device_Med_Sup_5': ['', '', ''],
    })

    assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
    assert set(result['Drug_Biological_Device_Med_Sup_1'].values) == set(expected_result['Drug_Biological_Device_Med_Sup_1'].values)
    assert set(result['Drug_Biological_Device_Med_Sup_2'].values) == set(expected_result['Drug_Biological_Device_Med_Sup_2'].values)
    assert set(result['Drug_Biological_Device_Med_Sup_3'].values) == set(expected_result['Drug_Biological_Device_Med_Sup_3'].values)
    assert set(result['Drug_Biological_Device_Med_Sup_4'].values) == set(expected_result['Drug_Biological_Device_Med_Sup_4'].values)
    assert set(result['Drug_Biological_Device_Med_Sup_5'].values) == set(expected_result['Drug_Biological_Device_Med_Sup_5'].values)


class TestAddNpis2014:
    def test_add_npis_2014_general(tmp_path):
        test_df = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['321', '654', '987'],
            'Other_Col': [0, 1, 2]
        })

        providers_npis_ids = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['321', '654', '987'],
            'Covered_Recipient_NPI': ['123', '456', '789']
        })

        id_cols = ['Covered_Recipient_Profile_ID']

        result = add_npis_2014(test_df, 'general', id_cols, providers_npis_ids)

        expected_result = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['321', '654', '987'],
            'Other_Col': [0, 1, 2],
            'Covered_Recipient_NPI': ['123', '456', '789']
        })

        assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
        assert set(result['Covered_Recipient_NPI'].values) == set(expected_result['Covered_Recipient_NPI'].values)
        assert set(result['Other_Col'].values) == set(expected_result['Other_Col'].values)


    def test_add_npis_2014_research(tmp_path):
        test_df = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['1', '', ''],
            'PI_1_Profile_ID': ['', '2', ''],
            'PI_2_Profile_ID': ['', '', '3'],
            'PI_3_Profile_ID': ['', '', ''],
            'PI_4_Profile_ID': ['', '', ''],
            'PI_5_Profile_ID': ['', '', ''],
            'Other_Col': [0, 1, 2]
        })

        providers_npis_ids = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['1', '2', '3'],
            'Covered_Recipient_NPI': ['123', '456', '789']
        })

        id_cols = [
            'Covered_Recipient_Profile_ID',
            'PI_1_Profile_ID',
            'PI_2_Profile_ID',
            'PI_3_Profile_ID',
            'PI_4_Profile_ID',
            'PI_5_Profile_ID'
            ]

        result = add_npis_2014(test_df, 'research', id_cols, providers_npis_ids)

        expected_result = pd.DataFrame(
            {
            'Covered_Recipient_Profile_ID': ['1', '', ''],
            'Covered_Recipient_NPI': ['123', '', ''],
            'PI_1_Profile_ID': ['', '2', ''],
            'PI_1_NPI': ['', '456', ''],
            'PI_2_Profile_ID': ['', '', '3'],
            'PI_2_NPI': ['', '', '789'],    
            'PI_3_Profile_ID': ['', '', ''],
            'PI_3_NPI': ['', '', ''],
            'PI_4_Profile_ID': ['', '', ''],
            'PI_4_NPI': ['', '', ''],
            'PI_5_Profile_ID': ['', '', ''],
            'PI_5_NPI': ['', '', ''],
            'Other_Col': [0, 1, 2]
            }
        )

        assert set(result.columns.to_list()) == set(expected_result.columns.to_list())
        assert set(result['Covered_Recipient_NPI'].values) == set(expected_result['Covered_Recipient_NPI'].values)
        assert set(result['Other_Col'].values) == set(expected_result['Other_Col'].values)


class TestCleanOpData:
    def test_clean_op_data_general(self, tmp_path):
        test_data = pd.DataFrame({
            'Covered_Recipient_NPI': ['123', '456', '789'],
            'Covered_Recipient_Profile_ID': ['1', '2', '3'],
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1': ['Trelstar', 'Pluvicto', 'DRUG_C'],
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2': ['DRUG_D', '', 'Rubraca'],
        })
        test_data.to_csv(tmp_path / "test_data.csv", index=False)
        path_file_to_clean = tmp_path / "test_data.csv"

        fileout_final_file = tmp_path / "test_cleaned_op_data.csv"
        filename = "test_cleaned_op_data.csv"

        year = 2016
        npi_set = ['123', '456']
        dataset_type = 'general'

        harmonized_cols = pd.DataFrame({
            '2016': ['Covered_Recipient_NPI',
                     'Covered_Recipient_Profile_ID',
                     'Drug_Biological_Device_Med_Sup_1',
                     'Drug_Biological_Device_Med_Sup_2',
                     ]
        })
        harmonized_cols.to_csv(tmp_path / "test_harmonized_cols.csv", index=False)
        path_to_harmonized_cols = tmp_path / "test_harmonized_cols.csv"

        providers_npis_ids = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['1', '2', '3'],
            'Covered_Recipient_NPI': ['123', '456', '789']
        })
        providers_npis_ids.to_csv(tmp_path / "test_providers_npis_ids.csv", index=False)
        path_providers_npis_ids = tmp_path / "test_providers_npis_ids.csv"

        dir_missing_npis = tmp_path / "test_missing_npis"
        dir_missing_npis.mkdir(parents=True, exist_ok=True)
        
        clean_op_data(
            path_file_to_clean,
            fileout_final_file,
            filename,
            year,
            npi_set,
            dataset_type,
            path_to_harmonized_cols,
            path_providers_npis_ids,
            dir_missing_npis
        )
        
        result = pd.read_csv(fileout_final_file).fillna('')
        
        expected_result = pd.DataFrame({
            'Covered_Recipient_NPI': ['123', '456', '789'],
            'Covered_Recipient_Profile_ID': ['1', '2', '3'],
            'Drug_Biological_Device_Med_Sup_1': ['Trelstar', 'Pluvicto', 'DRUG_C'],
            'Drug_Biological_Device_Med_Sup_2': ['DRUG_D', '', 'Rubraca'],
            'Drug_Name': ['triptorelin', 'psmalutetium177', 'rucaparib'],
            'Prostate_Drug_Type': [1, 0, 1],
            'Onc_Prescriber': [1, 0, 0]
        })

        assert fileout_final_file.exists()
        assert result.columns.to_list() == expected_result.columns.to_list()
        assert set(result['Drug_Name'].values) == set(expected_result['Drug_Name'].values)
        assert set(result['Prostate_Drug_Type'].values) == set(expected_result['Prostate_Drug_Type'].values)
        assert set(result['Onc_Prescriber'].values) == set(expected_result['Onc_Prescriber'].values)
    
    def test_clean_op_data_research(self, tmp_path):
        test_data = pd.DataFrame({
            'Covered_Recipient_NPI': ['123', '', ''],
            'Covered_Recipient_Profile_ID': ['1', '', ''],
            'Principal_Investigator_1_Profile_ID': ['', '2', ''],
            'Principal_Investigator_1_NPI': ['', '456', ''],
            'Principal_Investigator_2_Profile_ID': ['', '', '3'],
            'Principal_Investigator_2_NPI': ['', '', '789'],
            'Principal_Investigator_3_Profile_ID': ['', '', ''],
            'Principal_Investigator_3_NPI': ['', '', ''],
            'Principal_Investigator_4_Profile_ID': ['', '', ''],
            'Principal_Investigator_4_NPI': ['', '', ''],
            'Principal_Investigator_5_Profile_ID': ['', '', ''],
            'Principal_Investigator_5_NPI': ['', '', ''],
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1': ['Trelstar', 'Pluvicto', 'DRUG_C'],
            'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2': ['DRUG_D', '', 'Rubraca'],
        })
        test_data.to_csv(tmp_path / "test_data.csv", index=False)
        path_file_to_clean = tmp_path / "test_data.csv"

        fileout_final_file = tmp_path / "test_cleaned_op_data.csv"
        filename = "test_cleaned_op_data.csv"

        year = 2016
        npi_set = ['123', '456']
        dataset_type = 'research'

        harmonized_cols = pd.DataFrame({
            '2016': ['Covered_Recipient_NPI',
                     'Covered_Recipient_Profile_ID',
                     'PI_1_Profile_ID',
                     'PI_1_NPI',
                     'PI_2_Profile_ID',
                     'PI_2_NPI',
                     'PI_3_Profile_ID',
                     'PI_3_NPI',
                     'PI_4_Profile_ID',
                     'PI_4_NPI',
                     'PI_5_Profile_ID',
                     'PI_5_NPI',
                     'Drug_Biological_Device_Med_Sup_1',
                     'Drug_Biological_Device_Med_Sup_2',
                     ]
        })
        harmonized_cols.to_csv(tmp_path / "test_harmonized_cols.csv", index=False)
        path_to_harmonized_cols = tmp_path / "test_harmonized_cols.csv"

        providers_npis_ids = pd.DataFrame({
            'Covered_Recipient_Profile_ID': ['1', '2', '3'],
            'Covered_Recipient_NPI': ['123', '456', '789']
        })
        providers_npis_ids.to_csv(tmp_path / "test_providers_npis_ids.csv", index=False)
        path_providers_npis_ids = tmp_path / "test_providers_npis_ids.csv"

        dir_missing_npis = tmp_path / "test_missing_npis"
        dir_missing_npis.mkdir(parents=True, exist_ok=True)
        
        clean_op_data(
            path_file_to_clean,
            fileout_final_file,
            filename,
            year,
            npi_set,
            dataset_type,
            path_to_harmonized_cols,
            path_providers_npis_ids,
            dir_missing_npis
        )
        
        result = pd.read_csv(fileout_final_file).fillna('')
        
        expected_result = pd.DataFrame({
            'Covered_Recipient_NPI': ['123', '', ''],
            'Covered_Recipient_Profile_ID': ['1', '', ''],
            'PI_1_Profile_ID': ['', '2', ''],
            'PI_1_NPI': ['', '456', ''],
            'PI_2_Profile_ID': ['', '', '3'],
            'PI_2_NPI': ['', '', '789'],
            'PI_3_Profile_ID': ['', '', ''],
            'PI_3_NPI': ['', '', ''],
            'PI_4_Profile_ID': ['', '', ''],
            'PI_4_NPI': ['', '', ''],
            'PI_5_Profile_ID': ['', '', ''],
            'PI_5_NPI': ['', '', ''],
            'Drug_Biological_Device_Med_Sup_1': ['Trelstar', 'Pluvicto', 'DRUG_C'],
            'Drug_Biological_Device_Med_Sup_2': ['DRUG_D', '', 'Rubraca'],
            'Drug_Name': ['triptorelin', 'psmalutetium177', 'rucaparib'],
            'Prostate_Drug_Type': [1, 0, 1],
            'Onc_Prescriber': [1, 0, 0]
        })

        assert fileout_final_file.exists()
        assert result.columns.to_list() == expected_result.columns.to_list()
        assert set(result['Drug_Name'].values) == set(expected_result['Drug_Name'].values)
        assert set(result['Prostate_Drug_Type'].values) == set(expected_result['Prostate_Drug_Type'].values)
        assert set(result['Onc_Prescriber'].values) == set(expected_result['Onc_Prescriber'].values)
