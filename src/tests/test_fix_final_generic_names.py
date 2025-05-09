import os
import pytest
import pandas as pd

from src.fix_final_generic_names import (
    get_final_generic_names,
    replace_generic_names,
    get_final_files
)


def test_get_final_generic_names():
    expected_map = {
        'docetaxel': 'Docetaxel',
        'bicalutamide': 'Bicalutamide',
        'leuprolide': 'Leuprolide',
        'enzalutamide': 'Enzalutamide',
        'radium223': 'Radium 223',
        'sipuleucelt': 'Sipuleucel-T',
        'cabazitaxel': 'Cabazitaxel',
        'goserelin': 'Goserelin',
        'triptorelin': 'Triptorelin',
        'abiraterone': 'Abiraterone',
        'apalutamide': 'Apalutamide',
        'darolutamide': 'Darolutamide',
        'olaparib': 'Olaparib',
        'rucaparib': 'Rucaparib',
        'talazoparib': 'Talazoparib',
        'psmalutetium177': 'PSMA-Lutetium-177'
    }
    result = get_final_generic_names("data/reference/ProstateDrugList.csv")
    assert expected_map == result


def test_replace_generic_names():
    test_data = pd.DataFrame({
        'Drug_Name': ['sipuleucelt', 'radium223', 'goserelin'],
        'Other_col': [0, 1, 2]
    })

    generics_map = {
        'sipuleucelt': 'Sipuleucel-T',
        'olaparib': 'Olaparib',
        'radium223': 'Radium 223',
        'goserelin': 'Goserelin',
    }

    expected_df = pd.DataFrame({
        'Drug_Name': ['Sipuleucel-T', 'Radium 223', 'Goserelin'],
        'Other_col': [0, 1, 2]
    })

    result = replace_generic_names(test_data, generics_map)

    assert all(expected_df.items()) == all(result.items())


def test_replace_generic_names_error():
    test_data = pd.DataFrame({
        'Generic_name': ['BadVal'],
        'Other_col': [0]
    })

    generics_map = {
        'sipuleucelt': 'Sipuleucel-T',
        'olaparib': 'Olaparib',
        'radium223': 'Radium 223',
        'goserelin': 'Goserelin',
    }

    with pytest.raises(Exception):
        result = replace_generic_names(test_data, generics_map)


def test_get_final_files(tmp_path):
    test_data = pd.DataFrame({
        'Drug_Name': ['sipuleucelt', 'radium223', 'goserelin'],
        'Other_col': [0, 1, 2]
    })
    test_data.to_csv(tmp_path / 'test_data.csv')

    generics_map = {
        'sipuleucelt': 'Sipuleucel-T',
        'olaparib': 'Olaparib',
        'radium223': 'Radium 223',
        'goserelin': 'Goserelin',
    }

    dir_out = tmp_path / 'output'
    dir_out.mkdir()

    get_final_files(
        file_path=tmp_path / 'test_data.csv',
        generics_cleaned2final=generics_map,
        dir_out=f"{dir_out}/"
    )

    expected_df = pd.DataFrame({
        'Drug_Name': ['Sipuleucel-T', 'Radium 223', 'Goserelin'],
        'Other_col': [0, 1, 2]
    })

    assert os.path.exists(f"{dir_out}/test_data_final.csv")

    result = pd.read_csv(f"{dir_out}/test_data_final.csv")

    assert all(result.items()) == all(expected_df.items())

