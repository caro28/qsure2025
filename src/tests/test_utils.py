import unittest
import math
import tempfile
import shutil
import os
import pandas as pd
from src._utils import (
    clean_brand_name,
    clean_generic_name,
    concatenate_chunks,
    find_matches
)



class TestCleanBrandName(unittest.TestCase):
    def test_case_normalization(self):
        self.assertEqual(clean_brand_name("KEYTRUDA"), "keytruda")
        self.assertEqual(clean_brand_name("Xtandi"), "xtandi")
        self.assertEqual(clean_brand_name("Nubeqa"), "nubeqa")

    def test_empty_and_invalid_inputs(self):
        self.assertEqual(clean_brand_name(""), "")
        self.assertEqual(clean_brand_name(None), "")
        self.assertEqual(clean_brand_name(123), "")
        self.assertEqual(clean_brand_name(math.nan), "")  # Handle nan values
        self.assertEqual(clean_brand_name(float('nan')), "")  # Handle nan values

    def test_whitespace_handling(self):
        self.assertEqual(clean_brand_name("  LYNPARZA  "), "lynparza")
        self.assertEqual(clean_brand_name("NEW DRUG"), "newdrug")
        self.assertEqual(clean_brand_name("DRUG\tNAME"), "drugname")

    def test_punctuation_removal(self):
        self.assertEqual(clean_brand_name("DRUG-NAME"), "drugname")
        self.assertEqual(clean_brand_name("DRUG.NAME!"), "drugname")
        self.assertEqual(clean_brand_name("(DRUG)"), "drug")

    def test_unicode_normalization(self):
        # Test composed and decomposed forms of é
        self.assertEqual(clean_brand_name("CAFÉ"), "cafe")
        self.assertEqual(clean_brand_name("CAFE\u0301"), "cafe")
        # Test full-width characters
        self.assertEqual(clean_brand_name("ＤＲＵＧ"), "drug")
    
    def test_real_world_examples(self):
        # Test actual examples from your dataset
        test_cases = {
            "ERLEADA": "erleada",
            "PROVENGE": "provenge",
            "TALZENNA": "talzenna",
            "XTANDI": "xtandi",
            "INLYTA": "inlyta",
            "KEYTRUDA": "keytruda",
            "Nubeqa": "nubeqa",
            "LYNPARZA": "lynparza",
            "YONSA": "yonsa",
            "JEVTANA": "jevtana",
            "ELIGARD": "eligard",
            "CAMCEVI": "camcevi",
            "PLUVICTO": "pluvicto",
            "Triptodur": "triptodur",
            "Xofigo": "xofigo",
            "FENSOLVI": "fensolvi"
        }
        
        for input_name, expected in test_cases.items():
            with self.subTest(input_name=input_name):
                self.assertEqual(clean_brand_name(input_name), expected)

class TestCleanGenericName(unittest.TestCase):
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
            with self.subTest(input_name=input_name):
                self.assertEqual(clean_generic_name(input_name), expected)

    def test_trailing_token_variations(self):
        test_cases = {
            "Docetaxel  IV  ": "docetaxel",  # extra spaces
            "Abiraterone  Y  PO": "abiraterone",  # extra spaces between Y and PO
            "Goserelin   SUBQ": "goserelin",  # multiple spaces
            "Leuprolide\tIM": "leuprolide",  # tab instead of space
            "Bicalutamide\nPO": "bicalutamide",  # newline instead of space
        }
        for input_name, expected in test_cases.items():
            with self.subTest(input_name=input_name):
                self.assertEqual(clean_generic_name(input_name), expected)

    def test_case_variations(self):
        test_cases = {
            "DOCETAXEL IV": "docetaxel",
            "docetaxel IV": "docetaxel",
            "DoceTaxel IV": "docetaxel",
            "PSMA-LUTETIUM-177": "psmalutetium177",
            "psma-lutetium-177": "psmalutetium177"
        }
        for input_name, expected in test_cases.items():
            with self.subTest(input_name=input_name):
                self.assertEqual(clean_generic_name(input_name), expected)

    def test_empty_and_invalid_inputs(self):
        self.assertEqual(clean_generic_name(""), "")
        self.assertEqual(clean_generic_name(" "), "")
        self.assertEqual(clean_generic_name(None), "")
        self.assertEqual(clean_generic_name(123), "")

    def test_names_end_in_route_inputs(self):
        test_cases = {
            "IV": "iv",
            "PO": "po",
            "IM": "im",
            "SUBQ": "subq",
            "Y PO": "y"
        }
        for input_name, expected in test_cases.items():
            with self.subTest(input_name=input_name):
                self.assertEqual(clean_generic_name(input_name), expected)


class TestConcatenateChunks(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory
        self.test_dir = tempfile.mkdtemp()
        
    def tearDown(self):
        # Clean up
        shutil.rmtree(self.test_dir)

    def test_basic_concatenation(self):
        # Create two simple CSV files
        df1 = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        df2 = pd.DataFrame({'col1': [3, 4], 'col2': ['c', 'd']})
        
        # Save as chunks
        df1.to_csv(os.path.join(self.test_dir, 'chunk_1.csv'), index=False)
        df2.to_csv(os.path.join(self.test_dir, 'chunk_2.csv'), index=False)
        
        # Run concatenation
        output_file = os.path.join(self.test_dir, 'output.csv')
        concatenate_chunks(self.test_dir, output_file)
        
        # Check result
        result = pd.read_csv(output_file)
        expected = pd.DataFrame({
            'col1': [1, 2, 3, 4],
            'col2': ['a', 'b', 'c', 'd']
        })
        pd.testing.assert_frame_equal(result, expected)

    def test_single_chunk(self):
        # Test with just one file
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        df.to_csv(os.path.join(self.test_dir, 'chunk_1.csv'), index=False)
        
        output_file = os.path.join(self.test_dir, 'output.csv')
        concatenate_chunks(self.test_dir, output_file)
        
        result = pd.read_csv(output_file)
        pd.testing.assert_frame_equal(result, df)


class TestFindMatches(unittest.TestCase):
    def test_single_match(self):
        # Test finding a single match
        df = pd.DataFrame({
            'drug1': ['KEYTRUDA', 'DRUG_A'],
            'drug2': ['DRUG_B', 'DRUG_C']
        })
        
        matched_rows, row_idx = find_matches(
            df=df,
            drug_cols=['drug1', 'drug2'],
            drug_names=['keytruda']
        )
        
        self.assertEqual(matched_rows, 1)
        self.assertEqual(row_idx, [0])

    def test_match_order(self):
        # Test that we check all target names before moving to next column
        df = pd.DataFrame({
            'drug1': ['DRUG_A', 'XTANDI'],
            'drug2': ['KEYTRUDA', 'DRUG_B']
        })
        
        # Order is important - keytruda comes after xtandi in list
        matched_rows, row_idx = find_matches(
            df=df,
            drug_cols=['drug1', 'drug2'],
            drug_names=['xtandi', 'keytruda']
        )
        
        # Should find both XTANDI and KEYTRUDA
        self.assertEqual(matched_rows, 2)
        self.assertEqual(sorted(row_idx), [0, 1])

    def test_multiple_matches_same_row(self):
        # Test that we only count each row once even if multiple matches
        df = pd.DataFrame({
            'drug1': ['KEYTRUDA', 'XTANDI'],
            'drug2': ['KEYTRUDA', 'KEYTRUDA']  # Duplicate in same row
        })
        
        matched_rows, row_idx = find_matches(
            df=df,
            drug_cols=['drug1', 'drug2'],
            drug_names=['keytruda', 'xtandi']
        )
        
        # Should only count each row once
        self.assertEqual(matched_rows, 2)
        self.assertEqual(sorted(row_idx), [0, 1])





if __name__ == '__main__':
    unittest.main()