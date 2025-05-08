import unittest
import math
import pandas as pd
import numpy as np
from src._utils import (
    clean_brand_name,
    clean_generic_name,
    concatenate_chunks,
)



class TestCleanBrandName():
    def test_case_normalization(self):
        assert clean_brand_name("KEYTRUDA") == "keytruda"
        assert clean_brand_name("Xtandi") == "xtandi"
        assert clean_brand_name("Nubeqa") == "nubeqa"

    def test_empty_and_invalid_inputs(self):
        assert clean_brand_name("") == ""
        assert clean_brand_name(None) == ""
        assert clean_brand_name(123) == ""
        assert clean_brand_name(math.nan) == ""  # Handle nan values
        assert clean_brand_name(pd.NA) == ""  # Handle nan values
        assert clean_brand_name(np.nan) == ""  # Handle nan values
        assert clean_brand_name(float('nan')) == ""  # Handle nan values

    def test_whitespace_handling(self):
        assert clean_brand_name("  LYNPARZA  ") == "lynparza"
        assert clean_brand_name("NEW DRUG") == "newdrug"
        assert clean_brand_name("DRUG\tNAME") == "drugname"

    def test_punctuation_removal(self):
        assert clean_brand_name("DRUG-NAME") == "drugname"
        assert clean_brand_name("DRUG.NAME!") == "drugname"
        assert clean_brand_name("(DRUG)") == "drug"

    def test_unicode_normalization(self):
        # Test composed and decomposed forms of é
        assert clean_brand_name("CAFÉ") == "cafe"
        assert clean_brand_name("CAFE\u0301") == "cafe"
        # Test full-width characters
        assert clean_brand_name("ＤＲＵＧ") == "drug"
    
    def test_real_world_examples(self):
        # Test actual examples from your dataset
        test_cases = {
            "ERLEADA": "erleada",
            "Provenge": "provenge",
            "Talzenna": "talzenna",
            "Xtandi": "xtandi",
            "Inlyta": "inlyta",
            "Keytruda": "keytruda",
            "Nubeqa": "nubeqa",
            "Lynparza": "lynparza",
            "Yonsa": "yonsa",
            "Jevtana": "jevtana",
            "Eligard": "eligard",
            "Camcevi": "camcevi",
            "Pluvicto": "pluvicto",
            "Triptodur": "triptodur",
            "Xofigo": "xofigo",
            "Fensolvi": "fensolvi"
        }
        
        for input_name, expected in test_cases.items():
            assert clean_brand_name(input_name) == expected

class TestCleanGenericName():
    def test_real_world_inputs(self):
        test_cases = {
            "Docetaxel IV": "docetaxel",
            "Bicalutamide PO": "bicalutamide",
            "Leuprolide IM": "leuprolide",
            "Enzalutamide PO": "enzalutamide",
            "Radium 223 IV": "radium223",
            "Sipuleucel-T IV": "sipuleucelt",
            "Cabazitaxel IV": "cabazitaxel",
            "Goserelin SUBQ": "goserelin",
            "Triptorelin IM": "triptorelin",
            "Abiraterone Y PO": "abiraterone",
            "Apalutamide PO": "apalutamide",
            "Darolutamide PO": "darolutamide",
            "Olaparib PO": "olaparib",
            "Rucaparib PO": "rucaparib",
            "Talazoparib PO": "talazoparib",
            "PSMA-Lutetium-177": "psmalutetium177"
        }
        for input_name, expected in test_cases.items():
            assert clean_generic_name(input_name) == expected

    def test_trailing_token_variations(self):
        test_cases = {
            "Docetaxel  IV  ": "docetaxel",  # extra spaces
            "Abiraterone  Y  PO": "abiraterone",  # extra spaces between Y and PO
            "Goserelin   SUBQ": "goserelin",  # multiple spaces
            "Leuprolide\tIM": "leuprolide",  # tab instead of space
            "Bicalutamide\nPO": "bicalutamide",  # newline instead of space
        }
        for input_name, expected in test_cases.items():
            assert clean_generic_name(input_name) == expected

    def test_case_variations(self):
        test_cases = {
            "DOCETAXEL IV": "docetaxel",
            "docetaxel IV": "docetaxel",
            "DoceTaxel IV": "docetaxel",
            "PSMA-LUTETIUM-177": "psmalutetium177",
            "psma-lutetium-177": "psmalutetium177"
        }
        for input_name, expected in test_cases.items():
            assert clean_generic_name(input_name) == expected

    def test_empty_and_invalid_inputs(self):
        assert clean_generic_name("") == ""
        assert clean_generic_name(" ") == ""
        assert clean_generic_name(None) == ""
        assert clean_generic_name(123) == ""
        assert clean_generic_name(math.nan) == ""  # Handle nan values
        assert clean_generic_name(pd.NA) == ""  # Handle nan values
        assert clean_generic_name(np.nan) == ""  # Handle nan values
        assert clean_generic_name(float('nan')) == ""  # Handle nan values

    def test_names_end_in_route_inputs(self):
        test_cases = {
            "IV": "iv",
            "PO": "po",
            "drugIM": "drugim",
            "drugSUBQ": "drugsubq",
            "Y PO": "y",
            "drug IV": "drug",
            "DrUg PO": "drug",
            "DRUG IM": "drug",
            "druG subq": "drug",
        }
        for input_name, expected in test_cases.items():
            assert clean_generic_name(input_name) == expected


class TestConcatenateChunks():
    def test_basic_concatenation(self, tmp_path):
        test_dir = tmp_path / "test_dir"
        test_dir.mkdir()
        # Create two simple CSV files
        df1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        df2 = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        
        # Save as chunks
        df1.to_csv(test_dir / 'chunk_1.csv', index=False)
        df2.to_csv(test_dir / 'chunk_2.csv', index=False)
        
        # Run concatenation
        output_file = test_dir / 'output.csv'
        concatenate_chunks(test_dir, output_file)
        
        # Check result
        result = pd.read_csv(output_file)
        expected = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': ['a', 'b', 'c', 'd']
        })
        assert result.equals(expected)

    def test_single_chunk(self, tmp_path):
        # Test with just one file
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        df.to_csv(tmp_path / 'chunk_1.csv', index=False)
        
        output_file = tmp_path / 'output.csv'
        concatenate_chunks(tmp_path, output_file)
        
        result = pd.read_csv(output_file)
        assert result.equals(df)




if __name__ == '__main__':
    unittest.main()