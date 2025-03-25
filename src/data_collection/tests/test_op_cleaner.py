import unittest
from unittest.mock import patch, MagicMock, mock_open
import os
import pandas as pd
from src.data_collection._op_get_recordids_drugcols import (
    get_data_slice, 
    load_datastore_uuids, 
    get_last_chunk_info,
    validate_chunk,
    save_and_validate_chunk,
    initialize_download_session
)

class TestOpGetRecordidsDrugcols(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures"""
        self.test_data = [
            {'record_id': f'id_{i}', 'drug_col_1': f'drug1_{i}'} 
            for i in range(500)
        ]
        self.test_df = pd.DataFrame(self.test_data)

    @patch('src.data_collection._op_get_recordids_drugcols.open', new_callable=mock_open, 
           read_data='{"2023 General Payment Data": "test-uuid"}')
    def test_load_datastore_uuids(self, mock_file):
        """Test loading datastore UUIDs from JSON file"""
        result = load_datastore_uuids()
        self.assertEqual(result, {"2023 General Payment Data": "test-uuid"})
        mock_file.assert_called_once_with("data/reference/all_datastore_uuids.json", "r")

    @patch('pandas.read_parquet')
    @patch('os.listdir')
    def test_get_last_chunk_info_empty(self, mock_listdir, mock_read_parquet):
        """Test get_last_chunk_info with no existing files"""
        mock_listdir.return_value = []
        
        chunk_num, offset, is_complete = get_last_chunk_info("test_dir", "test_prefix")
        
        self.assertEqual(chunk_num, 0)
        self.assertEqual(offset, 0)
        self.assertTrue(is_complete)
        mock_read_parquet.assert_not_called()

    @patch('pandas.read_parquet')
    @patch('os.listdir')
    def test_get_last_chunk_info_complete_chunk(self, mock_listdir, mock_read_parquet):
        """Test get_last_chunk_info with complete last chunk"""
        mock_listdir.return_value = ['test_prefix_chunk_001.parquet']
        mock_df = MagicMock()
        mock_df.__len__.return_value = 100_000
        mock_read_parquet.return_value = mock_df
        
        chunk_num, offset, is_complete = get_last_chunk_info("test_dir", "test_prefix")
        
        self.assertEqual(chunk_num, 1)
        self.assertEqual(offset, 100_000)
        self.assertTrue(is_complete)

    @patch('pandas.read_parquet')
    @patch('os.listdir')
    def test_get_last_chunk_info_incomplete_chunk(self, mock_listdir, mock_read_parquet):
        """Test get_last_chunk_info with incomplete last chunk"""
        mock_listdir.return_value = ['test_prefix_chunk_001.parquet']
        mock_df = MagicMock()
        mock_df.__len__.return_value = 50_000
        mock_read_parquet.return_value = mock_df
        
        chunk_num, offset, is_complete = get_last_chunk_info("test_dir", "test_prefix")
        
        self.assertEqual(chunk_num, 1)
        self.assertEqual(offset, 50_000)
        self.assertFalse(is_complete)

    def test_validate_chunk_unique_ids(self):
        """Test validate_chunk with unique record_ids"""
        with self.assertLogs() as logs:
            validate_chunk(self.test_df, 1)
            self.assertTrue(any("✓ All record_ids in chunk 1 are unique" in log for log in logs.output))

    def test_validate_chunk_duplicate_ids(self):
        """Test validate_chunk with duplicate record_ids"""
        # Create DataFrame with duplicate record_ids
        duplicate_data = self.test_data + [self.test_data[0]]
        duplicate_df = pd.DataFrame(duplicate_data)
        
        with self.assertLogs() as logs:
            validate_chunk(duplicate_df, 1)
            self.assertTrue(any("! Found 1 duplicate record_ids" in log for log in logs.output))

    @patch('src.data_collection._op_get_recordids_drugcols.save_chunk_to_parquet')
    @patch('pandas.read_parquet')
    def test_save_and_validate_chunk(self, mock_read_parquet, mock_save):
        """Test save_and_validate_chunk function"""
        mock_save.return_value = "test/path/chunk_001.parquet"
        mock_read_parquet.return_value = self.test_df
        
        filepath = save_and_validate_chunk(
            self.test_data, 1, "test_dir", "test_prefix"
        )
        
        self.assertEqual(filepath, "test/path/chunk_001.parquet")
        mock_save.assert_called_once()
        mock_read_parquet.assert_called_once()

    @patch('src.data_collection._op_get_recordids_drugcols._discover_drug_columns')
    @patch('os.makedirs')
    @patch('src.data_collection._op_get_recordids_drugcols.get_last_chunk_info')
    def test_initialize_download_session_new(self, mock_last_chunk, mock_makedirs, mock_discover):
        """Test initialize_download_session for new download"""
        mock_discover.return_value = ['drug_col_1']
        mock_last_chunk.return_value = (0, 0, True)
        
        output_dir, offset, chunk_num, uuid, cols = initialize_download_session(
            "2023 General Payment Data", "test_prefix"
        )
        
        self.assertEqual(offset, 0)
        self.assertEqual(chunk_num, 1)
        self.assertEqual(cols, "record_id,drug_col_1")
        mock_makedirs.assert_called_once()

    @patch('src.data_collection._op_get_recordids_drugcols._discover_drug_columns')
    @patch('os.makedirs')
    @patch('src.data_collection._op_get_recordids_drugcols.get_last_chunk_info')
    @patch('os.remove')
    def test_initialize_download_session_resume(self, mock_remove, mock_last_chunk, 
                                             mock_makedirs, mock_discover):
        """Test initialize_download_session for resume with incomplete chunk"""
        mock_discover.return_value = ['drug_col_1']
        mock_last_chunk.return_value = (2, 150_000, False)
        
        output_dir, offset, chunk_num, uuid, cols = initialize_download_session(
            "2023 General Payment Data", "test_prefix"
        )
        
        self.assertEqual(offset, 100_000)  # Should start at beginning of incomplete chunk
        self.assertEqual(chunk_num, 2)
        mock_remove.assert_called_once()  # Should remove incomplete chunk

    @patch('src.data_collection._op_get_recordids_drugcols.initialize_download_session')
    @patch('src.data_collection._op_get_recordids_drugcols._sql_query_by_col')
    @patch('src.data_collection._op_get_recordids_drugcols.save_and_validate_chunk')
    def test_get_data_slice_success(self, mock_save, mock_sql_query, mock_init):
        """Test successful data retrieval and chunking"""
        # Mock initialization
        mock_init.return_value = ("test_dir", 0, 1, "test-uuid", "record_id,drug_col_1")
        
        # Mock API responses
        mock_sql_query.side_effect = [
            self.test_data,  # First batch
            []  # End of data
        ]
        
        # Mock successful file saving
        mock_save.return_value = "test/path/chunk_001.parquet"
        
        output_dir = get_data_slice("2023 General Payment Data", "test_prefix")
        
        self.assertEqual(output_dir, "test_dir")
        mock_save.assert_called_once()
        self.assertEqual(mock_sql_query.call_count, 2)

    @patch('src.data_collection._op_get_recordids_drugcols.initialize_download_session')
    @patch('src.data_collection._op_get_recordids_drugcols._sql_query_by_col')
    @patch('src.data_collection._op_get_recordids_drugcols.save_and_validate_chunk')
    def test_get_data_slice_error_handling(self, mock_save, mock_sql_query, mock_init):
        """Test error handling in get_data_slice"""
        mock_init.return_value = ("test_dir", 0, 1, "test-uuid", "record_id,drug_col_1")
        mock_sql_query.side_effect = Exception("API Error")
        
        output_dir = get_data_slice("2023 General Payment Data", "test_prefix")
        
        self.assertEqual(output_dir, "test_dir")
        mock_save.assert_not_called()

if __name__ == '__main__':
    unittest.main() 