import json
import pandas as pd
from src.filter_prescribers import (
    add_years_to_raw_prescriber_chunks,
    find_matches_prescribers,
    filter_prescribers_by_drug_names,
    get_final_npis,
    get_set_npis,
)


def test_add_years_to_raw_prescriber_chunks(tmp_path):
    # Create input and output directories
    dir_in = tmp_path / "input"
    dir_out = tmp_path / "output"
    dir_in.mkdir()
    dir_out.mkdir()

    # Create test input files with different years
    test_files = {
        "2018_prescribers.csv": pd.DataFrame({
            'Prscrbr_NPI': ['NPI1', 'NPI2'],
            'Brnd_Name': ['Drug1', 'Drug2']
        }),
        "2019_prescribers.csv": pd.DataFrame({
            'Prscrbr_NPI': ['NPI3', 'NPI4'],
            'Brnd_Name': ['Drug3', 'Drug4']
        })
    }

    # Save test files
    for filename, df in test_files.items():
        df.to_csv(dir_in / filename, index=False, encoding='latin-1')

    # Run function
    add_years_to_raw_prescriber_chunks(str(dir_in), str(dir_out))

    # Verify results
    for filename in test_files.keys():
        # Check output file exists
        output_file = dir_out / filename
        assert output_file.exists()

        # Read and verify content
        result_df = pd.read_csv(output_file)
        expected_year = filename.split('_')[0]
        
        # Check Year column was added with correct value
        assert 'Year' in result_df.columns

        # Convert both to strings for comparison
        assert all(result_df['Year'].astype(str) == str(expected_year))

        # Check original columns are preserved
        original_cols = test_files[filename].columns
        for col in original_cols:
            assert col in result_df.columns
        
        # Check number of rows is the same
        assert len(result_df) == len(test_files[filename])


class TestFindMatchesPrescribers():
    def test_find_matches_prescribers_matched(self):
        # mock ref_drug_names
        ref_drug_names = ['bicalutamide', 'enzalutamide']
        # mock drug_cols
        drug_cols = ['Brnd_Name', 'Gnrc_Name']
        # mock chunk
        chunk = pd.DataFrame({
            'Brnd_Name': ['bicalutamide', 'aspirin', 'enzalutamide', 'tylenol'],
            'Gnrc_Name': ['drug1', 'drug2', 'drug3', 'bicalutamide'],
            'Other_Col': [1, 2, 3, 4]
        })

        # run function
        filtered_chunk = find_matches_prescribers(chunk, drug_cols, ref_drug_names)
        assert len(filtered_chunk) == 3
        assert filtered_chunk['Other_Col'].to_list() == [1, 3, 4]
    
    def test_find_matches_prescribers_empty(self):
        # mock ref_drug_names
        ref_drug_names = ['bicalutamide', 'enzalutamide']
        # mock drug_cols
        drug_cols = ['Brnd_Name', 'Gnrc_Name']
        # mock chunk
        chunk = pd.DataFrame({
            'Brnd_Name': ['drug1', 'drug2', 'drug3', 'drug4'],
            'Gnrc_Name': ['drug1', 'drug2', 'drug3', 'drug4'],
            'Other_Col': [1, 2, 3, 4]
        })

        # run function
        filtered_chunk = find_matches_prescribers(chunk, drug_cols, ref_drug_names)
        assert len(filtered_chunk) == 0
        assert filtered_chunk.empty


class TestFilterPrescribersByDrugNames():
    def test_filter_prescribers_by_drug_names_matched(self, tmp_path):
        # mock test input file (prescribers_filtered_prscrb_type.csv)
        test_data = pd.DataFrame({
            'Brnd_Name': ['bicalutamide', 'aspirin', 'drug1', 'drug2'],
            'Gnrc_Name': ['drug3', 'drug4', 'enzalutamide', 'drug5'],
            'Other_Col': [1, 2, 3, 4]
        })        
        # save test data to input file
        test_data.to_csv(tmp_path / "prescribers_filtered_prscrb_type.csv", index=False)

        # mock dir_out
        dir_out = tmp_path / "output"
        dir_out.mkdir()
        
        # run function (add "/" to end of dir_out because function expects dir_out to end with "/")
        filter_prescribers_by_drug_names(tmp_path / "prescribers_filtered_prscrb_type.csv", f"{dir_out}/")

        # verify output
        assert (dir_out / "prescribers_chunk_1.csv").exists()
        result_df = pd.read_csv(dir_out / "prescribers_chunk_1.csv")
        assert len(result_df) == 2
        assert result_df['Other_Col'].tolist() == [1, 3]

    def test_filter_prescribers_by_drug_names_empty(self, tmp_path):
        # mock test input file (prescribers_filtered_prscrb_type.csv)
        test_data = pd.DataFrame({
            'Brnd_Name': ['drug1', 'drug2', 'drug3', 'drug4'],
            'Gnrc_Name': ['drug1', 'drug2', 'drug3', 'drug4'],
            'Other_Col': [1, 2, 3, 4]
        })        
        # save test data to input file
        test_data.to_csv(tmp_path / "prescribers_filtered_prscrb_type.csv", index=False)

        # mock dir_out
        dir_out = tmp_path / "output"
        dir_out.mkdir()
        
        # run function
        filter_prescribers_by_drug_names(tmp_path / "prescribers_filtered_prscrb_type.csv", dir_out)

        # verify output
        assert not (dir_out / "prescribers_chunk_1.csv").exists()


def test_get_final_npis(tmp_path):
    # Create test input DataFrame with proper data types
    test_data = pd.DataFrame({
        'Prscrbr_NPI': ['NPI1', 'NPI1', 'NPI1', 'NPI2', 'NPI1', 'NPI2'],
        'Prscrbr_Type': ['TypeA', 'TypeA', 'TypeA', 'TypeB', 'TypeB', 'TypeC'],
        'Brnd_Name': ['Drug1', 'Drug1', 'Drug2', 'Drug2', 'Drug1', 'Drug2'],
        'Gnrc_Name': ['Gen1', 'Gen1', 'Gen1', 'Gen2', 'Gen2', 'Gen2'],
        'Year': [2018, 2019, 2020, 2018, 2020, 2018]
    })

    # mock input and output paths
    test_input = str(tmp_path / "test_input.csv")
    test_output = str(tmp_path / "test_output.json")

    # save test data to input file
    test_data.to_csv(test_input, index=False)

    # run function
    get_final_npis(test_input, test_output)

    # verify output
    with open(test_output, 'r') as f:
        result_json = json.load(f)

    # verify dtyp of keys and values in json
    assert isinstance(result_json, dict)
    assert all(isinstance(key, str) for key in result_json.keys())
    assert all(isinstance(value, list) for value in result_json.values())
    assert all(isinstance(item, str) for sublist in result_json.values() for item in sublist)

    # verify only NPI1 is present
    assert result_json['2021'] == ['NPI1']

    # verify no duplicate NPIs
    assert len(result_json['2021']) == 1



