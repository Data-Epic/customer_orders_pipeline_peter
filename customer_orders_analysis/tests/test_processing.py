import sys
import os
#import dataframe from polars
from polars import DataFrame as PolarsDataFrame
import polars as pl
import numpy as np
from sqlalchemy.orm import declarative_base

# Adding the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
curr_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
data_dir = os.path.join(curr_dir, "data")
WORKING_DIR = "/customer_orders_analysis/data"

import unittest
from src.processing import (load_data,
                            check_missing_duplcates,
                            transform_product_category_df,
                            process_fact_table,
                            analysis,
                            load_to_duckdb 
                            )

orders_df_path =  f"{WORKING_DIR}/olist_order_items_dataset.csv"
order_items_df_path =  f"{WORKING_DIR}/olist_orders_dataset.csv"
customers_df_path =  f"{WORKING_DIR}/olist_customers_dataset.csv"
order_payments_df_path =  f"{WORKING_DIR}/olist_order_payments_dataset.csv"
products_df_path =  f"{WORKING_DIR}/olist_products_dataset.csv"
sellers_df_path =  f"{WORKING_DIR}/olist_sellers_dataset.csv"
product_category_df_path =  f"{WORKING_DIR}/product_category_name_translation.csv"

orders_df = load_data(orders_df_path)
order_items_df = load_data(order_items_df_path)
customers_df = load_data(customers_df_path)
order_payments_df = load_data(order_payments_df_path)
products_df = load_data(products_df_path)
sellers_df = load_data(sellers_df_path)
product_category_df = load_data(product_category_df_path)

class TestLoadData(unittest.TestCase):
    

    def test_load_data_none(self):
        "Test load_data function with None file_path"
        file_path = None
        result = load_data(file_path)
        self.assertEqual(result, "Invalid file path or format provided")
    
    def test_load_data_invalid(self):
        "Test load_data function with invalid file_path"
        file_path = "invalid_path"
        result = load_data(file_path)
        self.assertEqual(result, "File path does not exist or is not in the right format")
    
    def test_load_data_valid(self):
        "Test load_data function with valid file_path"

        file_path = os.path.join(data_dir, "olist_customers_dataset.csv")
        result = load_data(file_path).is_empty()  
        self.assertEqual(result, False)



class TestTransformProductCategoryDF(unittest.TestCase):

    def setUp(self):
        # Sample data for testing
        self.sample_data = pl.DataFrame({
            "product_category_name": ["category1", "category2", "category3"],
            "product_category_name_english": ["cat1", "cat2", "cat3"]
        })

    def test_with_correct_df(self):
        "Test with a valid product category df"
        file_path = os.path.join(data_dir, "product_category_name_translation.csv")
        df = load_data(file_path)
        result_df = transform_product_category_df(df)

        #check if the dataframe has the same no. of rows
        df_rows = len(df)
        result_rows = len(result_df)
        self.assertEqual(df_rows, result_rows)

        # Check if the result is a Polars DataFrame
        check_result = result_df.is_empty()
        self.assertEqual(check_result, False)

        # Check if the new column 'product_category_id' is added
        self.assertIn("product_category_id", result_df.columns)

        # Check if the new product_category_id column has the correct data type
        self.assertEqual(result_df["product_category_id"].dtype, pl.Int64)

        # Check if the new product_category_id column has the correct values
        expected_ids = pl.Series("product_category_id", 
                                 np.arange(1, len(df) + 1))
        equal_val = expected_ids == result_df["product_category_id"]
        
        self.assertEqual(equal_val.all(), True)

        # Check if the original product_category columns are preserved before the transformation
        original_columns = set(self.sample_data.columns)
        result_columns = set(result_df.columns) - {"product_category_id"}
        self.assertEqual(original_columns, result_columns)

    def test_empty_dataframe(self):
        "Test the transform category function parameter with an empty DataFrame"
        empty_df = pl.DataFrame()
        result_df = transform_product_category_df(empty_df)
        self.assertEqual(result_df, "Dataframe is empty")
    
    def test_with_string(self):
        "Test the transform category function parameter with a string type"

        result = transform_product_category_df("string")
        self.assertEqual(result, "Please provide a valid dataframe")
    
    def test_with_none(self):
        "Test the transform category function parameter with a None type"

        result = transform_product_category_df(None)
        self.assertEqual(result, "Please provide a valid dataframe")
    

class TestProcessFactTable(unittest.TestCase):

    def test_with_correct_data(self):
        "Test the process_fact_table function with with the valid list of dataframes"

        list_of_dfs = [orders_df,order_items_df, customers_df, order_payments_df, products_df, sellers_df, product_category_df]
        result = process_fact_table(list_of_dfs)
        self.assertIsInstance(result, pl.DataFrame)

    def test_empty_list(self):
        "Test the process_fact_table function with an empty list"
        empty_list = []
        result = process_fact_table(empty_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")
    
    def test_invalid_list(self):
        "Test the process_fact_table function with a list containing invalid data"
        invalid_list = ["string", "string", "string",
                         "string", "string", "string", "string"]
        result = process_fact_table(invalid_list)
        self.assertEqual(result, "Ensure you have a list of just polars dataframes")
    
    def test_none_type(self):
        "Test the process_fact_table function with a None type"
        result = process_fact_table(None)
        self.assertEqual(result, "Please provide a list of polars dataframes")
    
    def test_with_string(self):
        "Test the process_fact_table function with a string type"
        result = process_fact_table("string")
        self.assertEqual(result, "Please provide a list of polars dataframes")
    
    def test_with_int(self):
        "Test the process_fact_table function with an integer type"
        result = process_fact_table(123)
        self.assertEqual(result, "Please provide a list of polars dataframes")


class TestAnalysis(unittest.TestCase):
    
        def test_with_correct_data(self):
            "Test the analysis function with the valid fact table"

            list_of_dfs = [orders_df, order_items_df, customers_df,
                            order_payments_df, products_df, sellers_df, 
                            product_category_df]
            
            fact_table = process_fact_table(list_of_dfs)
            result = analysis(fact_table) 
            self.assertIsInstance(result, dict) 
        
        def test_empty_dataframe(self):
            "Test the analysis function parameter with an empty DataFrame"
            empty_df = pl.DataFrame()
            result = analysis(empty_df)
            self.assertEqual(result, "Dataframe is empty, Provide the valid Fact table")
        
        def test_with_string(self):
            "Test the analysis function parameter with a string type"
            result = analysis("string")
            self.assertEqual(result, "Please provide the valid Fact table")
        
        def test_with_none(self):
            "Test the analysis function parameter with a None type"
            result = analysis(None)
            self.assertEqual(result, "Please provide the valid Fact table")




if __name__ == "__main__":
    unittest.main()


