import unittest
import pandas as pd
import os
import shutil
import tempfile
from src.filter_prescribers import (
    filter_prescribers_by_drug_names,
    has_three_consecutive_years,
    get_final_npis,
    get_set_npis,
    add_years_to_raw_prescriber_chunks,
)


class TestFilterPrescribersByDrugNames(unittest.TestCase):
    def test_filter_prescribers_by_drug_names(self):
        # Create a temporary directory for output
        with tempfile.TemporaryDirectory() as temp_dir:
            # Add path separator to temp_dir
            temp_dir = f"{temp_dir}/"
            
            # Create a test input CSV file
            test_data = pd.DataFrame({
                'Brnd_Name': ['bicalutamide', 'aspirin', 'enzalutamide', 'tylenol'],
                'Gnrc_Name': ['drug1', 'drug2', 'drug3', 'drug4'],
                'Other_Col': [1, 2, 3, 4]
            })
            
            # Save test data to temporary CSV
            input_path = os.path.join(temp_dir, 'test_input.csv')
            test_data.to_csv(input_path, index=False)
            
            # Run the function
            filter_prescribers_by_drug_names(input_path, temp_dir)
            
            # Check if chunk file exists
            chunk_file = f"{temp_dir}prescribers_chunk_1.csv"
            assert os.path.exists(chunk_file)
            
            # Read and verify results
            result_df = pd.read_csv(chunk_file)
            assert len(result_df) == 2  # Should only have rows with bicalutamide and enzalutamide
            assert 'bicalutamide' in result_df['Brnd_Name'].values
            assert 'enzalutamide' in result_df['Brnd_Name'].values

    def test_empty_input(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            # Add path separator to temp_dir
            temp_dir = f"{temp_dir}/"
            
            # Create empty test input
            test_data = pd.DataFrame({
            'Brnd_Name': [],
            'Gnrc_Name': [],
            'Other_Col': []
        })
        
            input_path = os.path.join(temp_dir, 'test_input.csv')
            test_data.to_csv(input_path, index=False)
            
            # Run the function
            filter_prescribers_by_drug_names(input_path, temp_dir)
            
            # For empty input, no chunk files should be created
            chunk_file = f"{temp_dir}prescribers_chunk_1.csv"
            assert not os.path.exists(chunk_file)


def test_has_three_consecutive_years():
    # Test case with three consecutive years
    consecutive = pd.DataFrame({
        'Year': ['2018', '2019', '2020', '2021'],
    })
    assert has_three_consecutive_years(consecutive) is True

    # Test case with non-consecutive years
    non_consecutive = pd.DataFrame({
        'Year': ['2018', '2020', '2022'],
    })
    assert has_three_consecutive_years(non_consecutive) is False

    # Test case with exactly three consecutive years
    exact_three = pd.DataFrame({
        'Year': ['2018', '2019', '2020'],
    })
    assert has_three_consecutive_years(exact_three) is True

    # Test case with two years only
    two_years = pd.DataFrame({
        'Year': ['2018', '2019'],
    })
    assert has_three_consecutive_years(two_years) is False

    # Test case with duplicate years
    duplicate_years = pd.DataFrame({
        'Year': ['2018', '2018', '2019', '2020'],
    })
    assert has_three_consecutive_years(duplicate_years) is True

    # Test case with empty DataFrame
    empty_df = pd.DataFrame({
        'Year': [],
    })
    assert has_three_consecutive_years(empty_df) is False


def test_get_final_npis(tmp_path):
    # Create test input DataFrame with proper data types
    test_data = pd.DataFrame({
        'Prscrbr_NPI': ['NPI1', 'NPI1', 'NPI1', 'NPI2', 'NPI2', 'NPI3'],
        'Year': [2018, 2019, 2020, 2018, 2020, 2018],
        'Prscrbr_Type': ['Type1', 'Type1', 'Type1', 'Type2', 'Type2', 'Type3'],
        'Brnd_Name': ['Drug1', 'Drug2', 'Drug3', 'Drug4', 'Drug5', 'Drug6'],
        'Gnrc_Name': ['Gen1', 'Gen2', 'Gen3', 'Gen4', 'Gen5', 'Gen6'],
        'Extra_Col': ['X1', 'X2', 'X3', 'X4', 'X5', 'X6']
    })

    # Convert Year to string after DataFrame creation to ensure proper formatting
    test_data['Year'] = test_data['Year'].astype(str)

    # Create input and output paths
    test_input = str(tmp_path / "test_input.csv")
    test_output = str(tmp_path / "prescribers_final_npis.csv")

    # Save test data to input file
    test_data.to_csv(test_input, index=False)

    # Run function with input and output paths
    get_final_npis(test_input, test_output)

    # Read and verify results
    result_df = pd.read_csv(test_output)

    # Test correct columns are present
    expected_columns = ['Prscrbr_NPI', 'Prscrbr_Type', 'Brnd_Name', 'Gnrc_Name', 'Year']
    assert list(result_df.columns) == expected_columns

    # Test only NPI1 is present (only one with consecutive years)
    assert len(result_df) == 1
    assert result_df['Prscrbr_NPI'].iloc[0] == 'NPI1'

    # Test no duplicate NPIs
    assert len(result_df['Prscrbr_NPI'].unique()) == len(result_df)

    # Test extra columns were dropped
    assert 'Extra_Col' not in result_df.columns


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
        assert all(result_df['Year'].astype(str) == expected_year)

        # Check original columns are preserved
        original_cols = test_files[filename].columns
        for col in original_cols:
            assert col in result_df.columns


def test_get_set_npis(tmp_path):
    # Create test input DataFrame with unique NPIs
    test_data = pd.DataFrame({
        'Prscrbr_NPI': ['NPI1', 'NPI2', 'NPI3'],
        'Prscrbr_Type': ['Type1', 'Type2', 'Type3'],
        'Brnd_Name': ['Drug1', 'Drug2', 'Drug3'],
        'Gnrc_Name': ['Gen1', 'Gen2', 'Gen3']
    })

    # Create and save test input file
    test_input = tmp_path / "prescribers_final_npis.csv"
    test_data.to_csv(test_input, index=False)

    # Temporarily modify the function to use test path
    import src.filter_prescribers
    original_path = "data/filtered/prescribers/prescribers_final_npis.csv"
    src.filter_prescribers.npi_path = str(test_input)

    try:
        # Run function
        result = src.filter_prescribers.get_set_npis(str(test_input))
        print(result)

        # Test the results
        assert len(result) == 3
        assert set(result) == {'NPI1', 'NPI2', 'NPI3'}
        # Each NPI should appear exactly once
        assert len(result.value_counts().unique()) == 1
        assert result.value_counts().unique()[0] == 1

    finally:
        # Restore original path
        src.filter_prescribers.npi_path = original_path


if __name__ == '__main__':
    unittest.main()