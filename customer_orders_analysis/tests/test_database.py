import sys
import os
from unittest.mock import Mock, patch, MagicMock
import unittest
import polars as pl
# Adding the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
curr_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
data_dir = os.path.join(curr_dir, "data")

from src.processing import load_to_duckdb


class TestLoadToDuckDB(unittest.TestCase):
    def setUp(self):
        "Sample data for testing"
        self.df = pl.DataFrame({
            "id": [1, 2, 3, 4],
            "name": ["Sogo", "Toba", "Bush", "Tola"],
            "occupation": ["Engineer", "Doctor", "Nurse", "Teacher"]
        })
        self.db_path = "test_db.duckdb"
        # self.Base = declarative_base()
        self.model = Mock()
        self.Base = Mock()
        self.Base.metadata.create_all = Mock()
    
    @patch("src.processing.create_engine")
    @patch("src.processing.sessionmaker")
    @patch("src.processing.inspect")
    def test_load_to_duckdb(self, mock_inspect, mock_sessionmaker, mock_create_engine):
        "Test the load_to_duckdb function with sample data"
        mock_session = MagicMock()
        mock_sessionmaker.return_value.return_value = mock_session
        
        #mock the model's primary key
        mock_primary_key = MagicMock(name = 'id')
        mock_inspect.return_value.primary_key = [mock_primary_key]

        existing_record = MagicMock()
        existing_record.id = 1
        mock_session.query().return_value.all.return_value = [existing_record]
        
        load_to_duckdb(self.df, self.db_path, self.model, self.Base)

        # print("Number of add calls:", mock_session.add.call_count)
        # print("Add method calls:", mock_session.add.call_args_list)

        #assertions
        mock_create_engine.assert_called_once_with(f"duckdb:///{self.db_path}")
        self.Base.metadata.create_all.assert_called_once()
        mock_sessionmaker.assert_called_once()

        #check if the session was called correctly
        mock_session.query.assert_any_call(self.model)
        mock_session.add.assert_called()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()

        #check if the add method was called correctly
        self.assertEqual(mock_session.add.call_count, 4)
        # print("Add method calls:", mock_session.add.call_args_list)


    @patch('src.processing.create_engine')
    @patch('src.processing.sessionmaker')
    @patch('src.processing.inspect')
    def test_load_to_duckdb_empty_dataframe(self, mock_inspect, mock_sessionmaker, mock_create_engine):
        "Test the load_to_duckdb function with an empty DataFrame"
        # Mock the database session
        mock_session = MagicMock()
        mock_sessionmaker.return_value.return_value = mock_session

        # Mock the model's primary key
        mock_inspect.return_value.primary_key = [MagicMock(name='id')]

        # Create an empty DataFrame
        empty_df = pl.DataFrame()

        # Call the function
        load_to_duckdb(empty_df, self.db_path, self.model, self.Base)

        # Check if no records were added
        mock_session.add.assert_not_called()
        mock_session.commit.assert_called_once()
        mock_session.close.assert_called_once()