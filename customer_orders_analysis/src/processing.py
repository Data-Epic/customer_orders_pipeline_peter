import polars as pl
import os
import numpy as np
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import DeclarativeMeta


def load_data(file_path: str) -> pl.DataFrame:
    try:
        if isinstance(file_path, str):
            csv_format = file_path.endswith(".csv")
            excel_format = file_path.endswith(".xlsx")
            
            #if the file exists in the working directory
            if csv_format:
                data_read_csv = pl.read_csv(file_path)
                if data_read_csv.is_empty() == False:
                    return data_read_csv
                else:
                    return "Error reading csv file"
            elif excel_format:
                data_read_excel = pl.read_excel(file_path)
                if data_read_excel.is_empty() == False:
                    return data_read_excel
                else:
                    return "Error reading excel file"
            else:
                return "File path does not exist or is not in the right format"
        else:
            return "Invalid file path or format provided"

    except ValueError as e:
        print(f"An error occurred: {e}")
        return e
    


def check_missing_duplcates(df: pl.DataFrame, df_name: str):
    null_df = df.filter(df.is_empty() == True)
    null_count = null_df.shape[0]
    print(f"{df_name} Null count: {null_count}")
    unique_count = df.n_unique()
    filtered_df = df.filter(df.is_duplicated() == False)
    if unique_count == filtered_df.shape[0]:
        print(f"No duplicates found in {df_name}")


#i Want to link these tables together and establish a fact table
#I will join the dataframes on the common columns
#I wnant to create a unique for the product category dataframe,
#  so that we can join it with the other dataframes

def transform_product_category_df(df: pl.DataFrame) -> pl.DataFrame:
    if isinstance(df, pl.DataFrame):
        if df.is_empty() == False:
            col_name = "product_category_id"
            df = df.with_columns(pl.Series(col_name, np.arange(1, len(df) + 1)))
            df = df.with_columns(pl.col(col_name).cast(pl.Int64))
            return df
        elif df.is_empty() == True:
            return "Dataframe is empty"
    else:
        return "Please provide a valid dataframe"
  
        
def process_fact_table(list_of_dfs: list,
                       no_tables = 7) -> pl.DataFrame:

    if isinstance(list_of_dfs, list) == True:
         
        all_dfs = []
        if len(list_of_dfs) != 0:
            for df in list_of_dfs:
                if isinstance(df, pl.DataFrame):
                    all_dfs.append(True)

        if len(all_dfs) == no_tables: 
            orders_df , \
            order_items_df, \
            customers_df,order_payments_df,products_df,sellers_df, product_category_df = list_of_dfs
            
            product_category_df = transform_product_category_df(product_category_df)

            fact_table = orders_df.join(
                order_items_df, on="order_id", how="inner"  
                        ). \
                join(customers_df, on="customer_id", how="inner") .\
                join(order_payments_df, on="order_id", how="inner")\
                .join(products_df.select(["product_id", "product_category_name"])
                ,on="product_id", how="inner") .\
                join(sellers_df, on="seller_id", how="inner"). \
                join(product_category_df, on=["product_category_name"], how="inner")
            fact_table = fact_table.head(100)
            
            fact_table = fact_table.with_columns(pl.Series("id", np.arange(1, len(fact_table) + 1)))
            fact_table = fact_table.with_columns(pl.col("id").cast(pl.Int64))

            return fact_table

        else:
            return "Ensure you have a list of just polars dataframes"
    else:
        return "Please provide a list of polars dataframes"

        
        

#analysis
#i want to get the top selling sellers
#i want to get the most used payment type
#I want to get the top loyal customers
#I want to get the top selling product category
#I want to get  the order status count 
#I want to get the average delivery time for each product category
#I want to get the average payment value by payment type

def analysis(df: pl.DataFrame):
    
    if isinstance(df, pl.DataFrame) == True:
        if df.is_empty() == False:
            #top selling sellers
            top_selling_sellers = df.group_by("seller_id").agg([
                pl.sum("price").alias("Total_sales")
            ]).sort("Total_sales", descending = True).select(["seller_id", "Total_sales"]).head(10)

            top_selling_sellers = top_selling_sellers.with_columns(pl.Series("id",
                                                                            np.arange(1, len(top_selling_sellers) + 1)))
            top_selling_sellers = top_selling_sellers.with_columns(pl.col("id").cast(pl.Int64))
            # print(top_selling_sellers)

            #most used payment_type
            most_used_payment_type = df.group_by("payment_type").agg([
                pl.count("payment_type").alias("Count")
            ]).sort("Count", descending = True).select(["payment_type", "Count"]).head(10)

            most_used_payment_type = most_used_payment_type.with_columns(pl.Series("id", np.arange(1, len(most_used_payment_type) + 1)))
            most_used_payment_type = most_used_payment_type.with_columns(pl.col("id").cast(pl.Int64))

            #top selling product category
            top_selling_product_category = df.group_by("product_category_name_english").agg([
                pl.sum("price").alias("Total_sales")
            ]).sort("Total_sales", descending = True) \
                .select(["product_category_name_english", "Total_sales"]).head(10)
            top_selling_product_category = top_selling_product_category.with_columns(pl.Series("id", np.arange(1, len(top_selling_product_category) + 1)))
            top_selling_product_category = top_selling_product_category.with_columns(pl.col("id").cast(pl.Int64))


            #order status count
            order_status_count = df.group_by("order_status").agg([
                pl.count("order_status").alias("Count")
            ]).sort("Count", descending = True).select(["order_status", "Count"]).head(10)

            order_status_count = order_status_count.with_columns(pl.Series("id", np.arange(1, len(order_status_count) + 1)))
            order_status_count = order_status_count.with_columns(pl.col("id").cast(pl.Int64))

            #top average delivery time for each product category
            delivery_time = df. \
                with_columns((pl.col("order_delivered_customer_date").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S") \
                            - pl.col("order_purchase_timestamp").str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S")) \
                                .alias("delivery_duration"))
            
            
            #convert the delivery duration to days
            delivery_time = delivery_time.with_columns((pl.col("delivery_duration") / (24 * 60 * 60*1000000)) \
                                                    .cast(pl.Float64).alias("delivery_duration_days")) 
            

            delivery_time = delivery_time.group_by("product_category_name_english") \
                    .agg([pl.col("delivery_duration_days").mean().alias("Average_delivery_duration_days")]) \
                            .select(["product_category_name_english", "Average_delivery_duration_days"]) \
                                .sort("Average_delivery_duration_days", descending = True).head(10)

            delivery_time = delivery_time.with_columns(pl.Series("id", np.arange(1, len(delivery_time) + 1)))
            delivery_time = delivery_time.with_columns(pl.col("id").cast(pl.Int64))


            average_payment_value = df.group_by("payment_type").agg([
                pl.mean("payment_value").alias("Average_payment_value")
            ]). \
                select(["payment_type", "Average_payment_value"]) \
                    .sort("Average_payment_value", descending = True).head(10)
            average_payment_value = average_payment_value.with_columns(pl.Series("id", np.arange(1, len(average_payment_value) + 1)))
            average_payment_value = average_payment_value.with_columns(pl.col("id").cast(pl.Int64))


            #top loyal customers
            loyal_customers = df.group_by("customer_unique_id").agg([
                pl.count("customer_id").alias("no_of_orders"),
            ]).sort("no_of_orders", descending = True).head(10)

            loyal_customers = loyal_customers.with_columns(pl.Series("id", np.arange(1, len(loyal_customers) + 1)))
            loyal_customers = loyal_customers.with_columns(pl.col("id").cast(pl.Int64))


            return {
                "top_selling_sellers": top_selling_sellers,
                "most_used_payment_type": most_used_payment_type,
                "top_selling_product_category": top_selling_product_category,
                "order_status_count": order_status_count,
                "average_delivery_time": delivery_time,
                "average_payment_value": average_payment_value,
                "loyal_customers": loyal_customers
            
            }
    
        else :
            return "Dataframe is empty, Provide the valid Fact table"
    else:
        return "Please provide the valid Fact table"


def load_to_duckdb(df: pl.DataFrame,
                    db_path: str,
                      model: declarative_base,
                        Base: declarative_base):

        engine = create_engine(f"duckdb:///{db_path}")
        Base.metadata.create_all(engine)
        Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        
        db = Session()

        new_df_dict = {}
        df_columns = df.columns
        primary_key_col = [col.name for col in inspect(model).primary_key]
        primary_key_col = primary_key_col[0]

        def query_to_dict(model, records):
            # Get the primary key column name
            primary_key = inspect(model).primary_key[0].name
            # Convert to dictionary
            return {getattr(record, primary_key): record for record in records}
        
        existing_records = db.query(model).all() #Query the database for existing records

        records_dict = query_to_dict(model, existing_records) # Convert the existing records to a dictionary

        try:
            new_df_dict = {}
            all_records = []
            for row in df.iter_rows():
                num = 0
                for col in df_columns:
                    new_df_dict[col] = row[df_columns.index(col)]
                    num += 1
                    if num % len(df_columns) == 0:
                        all_records.append(new_df_dict.copy())  #get the dataframe records into a list of dictionaries,
                                                                #each dictionary represents a row in the dataframe

            ##print("new_df_dict", all_records)
            #print("record values", records_dict.values())
            #print("all records", all_records)

            for record in all_records:
                try: 
                    if record.get(primary_key_col) not in records_dict.keys(): # Check if the record already exists in the database using the primary key
                        new_df_dict = {col: record[col] for col in df_columns}
                        table = model(**new_df_dict)
                        db.add(table)
                    else: # If the record already exists, skip it and continue to the next record
                        db.rollback()
                        continue
                except Exception as e:
                    print(f"Error: {e}")
                    db.rollback()
                    continue

            db.commit()
            print(f"{model} saved to {db_path} successfully")
        except Exception as e:
            db.rollback()
            print(f"An error occurred: {e}")
            raise
        finally:
            db.close()
