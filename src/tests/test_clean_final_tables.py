import pandas as pd
import pytest
from src.clean_final_tables import add_new_columns

# Mock the dependencies
@pytest.fixture
def mock_brand2generic(monkeypatch):
    mock_map = {
        'DRUG_A': 'Docivyx',
        'DRUG_B': 'Eligard',
        'generic_a': 'Docetaxel',
        'generic_b': 'Leuprolide'
    }
    def mock_build_ref_data_maps():
        return mock_map, {'DRUG_A': 'yellow', 'DRUG_B': 'green', 'generic_a': 'yellow', 'generic_b': 'green'}
    monkeypatch.setattr('src.clean_final_tables.build_ref_data_maps', mock_build_ref_data_maps)

@pytest.fixture
def mock_npi_set(monkeypatch):
    def mock_get_set_npis(path):
        return {'123', '456'}
    monkeypatch.setattr('src.clean_final_tables.get_set_npis', mock_get_set_npis)

def test_add_new_columns_basic(mock_brand2generic, mock_npi_set):
    # Create test input data
    df = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['DRUG_A', 'DRUG_B', ''],
        'Covered_Recipient_NPI': ['123', '456', '789']
    })
    
    # Run function
    result = add_new_columns(df, ['Drug_Biological_Device_Med_Sup_1'])
    import pdb; pdb.set_trace()
    
    # Check results
    assert 'Drug_Name' in result.columns
    assert 'Prostate_Drug_Type' in result.columns
    assert 'Onc_Prescriber' in result.columns
    
    # Check first row (DRUG_A - yellow drug, NPI in set)
    assert result.iloc[0]['Drug_Name'] == 'generic_a'
    assert result.iloc[0]['Prostate_Drug_Type'] == 1
    assert result.iloc[0]['Onc_Prescriber'] == 1
    
    # Check second row (DRUG_B - blue drug, NPI in set)
    assert result.iloc[1]['Drug_Name'] == 'generic_b'
    assert result.iloc[1]['Prostate_Drug_Type'] == 0
    assert result.iloc[1]['Onc_Prescriber'] == 0

def test_add_new_columns_empty_values(mock_brand2generic, mock_npi_set):
    df = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['', 'nan', None],
        'Covered_Recipient_NPI': ['123', '456', '789']
    })
    
    result = add_new_columns(df, ['Drug_Biological_Device_Med_Sup_1'])
    
    # Check that empty values don't cause errors
    assert 'Drug_Name' in result.columns
    assert 'Prostate_Drug_Type' in result.columns
    assert 'Onc_Prescriber' in result.columns

def test_add_new_columns_unknown_drug(mock_brand2generic, mock_npi_set):
    df = pd.DataFrame({
        'Drug_Biological_Device_Med_Sup_1': ['UNKNOWN_DRUG'],
        'Covered_Recipient_NPI': ['123']
    })
    
    result = add_new_columns(df, ['Drug_Biological_Device_Med_Sup_1'])
    
    # Check that unknown drugs are handled gracefully
    assert 'Drug_Name' in result.columns
    assert 'Prostate_Drug_Type' in result.columns
    assert 'Onc_Prescriber' in result.columns