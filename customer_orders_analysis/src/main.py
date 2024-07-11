from processing import (load_data,
                        check_missing_duplcates,
                        transform_product_category_df,
                        process_fact_table,
                        analysis,
                        load_to_duckdb)

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from database import (Base,
                        FactTable,
                        Top_Selling_Sellers,
                        Most_Used_Payment_Type,
                        Top_Selling_Product_Category,
                        Order_Status_Count,
                        Top_Average_Delivery_Time,
                        Average_Payment_Value,
                        Loyal_Customers
                        )

import os
db_path = "customer_orders.duckdb"
WORKING_DIR = "/customer_orders_analysis/data"

def main():

    df_path_dict = {
    "customers_df": f"{WORKING_DIR}/olist_customers_dataset.csv",
    "orders_df": f"{WORKING_DIR}/olist_order_items_dataset.csv",
    "order_items_df": f"{WORKING_DIR}/olist_orders_dataset.csv",
    "products_df": f"{WORKING_DIR}/olist_products_dataset.csv",
    "sellers_df": f"{WORKING_DIR}/olist_sellers_dataset.csv",
    "product_category_df": f"{WORKING_DIR}/product_category_name_translation.csv",
    "order_payments_df": f"{WORKING_DIR}/olist_order_payments_dataset.csv"
   }

    customers_df  = load_data(df_path_dict["customers_df"])
    orders_df = load_data(df_path_dict["orders_df"])
    order_items_df = load_data(df_path_dict["order_items_df"])
    products_df = load_data(df_path_dict["products_df"])
    sellers_df = load_data(df_path_dict["sellers_df"])
    product_category_df = load_data(df_path_dict["product_category_df"])
    order_payments_df = load_data(df_path_dict["order_payments_df"])

    print(customers_df.head())
    print(orders_df.head())
    print(order_items_df.head())
    print(products_df.head())
    print(sellers_df.head())
    print(product_category_df.head())
    print(order_payments_df.head())

    list_of_dfs = [orders_df, 
                   order_items_df,
                   customers_df,
                   order_payments_df, 
                   products_df,
                   sellers_df, 
                   product_category_df 
                   ]
    
    
    fact_table = process_fact_table(list_of_dfs)
    analytical_tables = analysis(fact_table)
    
    top_selling_sellers_table = analytical_tables["top_selling_sellers"]
    most_used_payment_type_table = analytical_tables["most_used_payment_type"]
    top_selling_product_category_table = analytical_tables["top_selling_product_category"]
    order_status_count_table = analytical_tables["order_status_count"]
    top_average_delivery_time_table = analytical_tables["average_delivery_time"]
    average_payment_value_table = analytical_tables["average_payment_value"]
    loyal_customers_table = analytical_tables["loyal_customers"]
    

    load_to_duckdb(
        fact_table,
        db_path,
        FactTable,
        Base
    )
    load_to_duckdb(top_selling_sellers_table, db_path, Top_Selling_Sellers, Base)
    load_to_duckdb(most_used_payment_type_table, db_path, Most_Used_Payment_Type, Base)
    load_to_duckdb(top_selling_product_category_table, db_path, Top_Selling_Product_Category, Base)
    load_to_duckdb(order_status_count_table, db_path, Order_Status_Count, Base)
    load_to_duckdb(top_average_delivery_time_table, db_path, Top_Average_Delivery_Time, Base)
    load_to_duckdb(average_payment_value_table, db_path, Average_Payment_Value, Base)
    load_to_duckdb(loyal_customers_table, db_path, Loyal_Customers, Base)

    
if __name__ == "__main__":
    main()