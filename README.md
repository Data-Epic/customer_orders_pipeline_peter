# Brazilian Customers Orders ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for the Brazilian E-Commerce Public Dataset by Olist. It uses Polars for data processing and DuckDB as the target database. The entire pipeline, including data ingestion scripts, unit tests, and the DuckDB database, is containerized using Docker for easy deployment and scalability.The Docker environment is designed to extract the  data and provide a command-line interface (CLI) for querying the data using DuckDB.

## Features

- Data extraction from multiple CSV files in the Olist dataset
- Data transformation and modeling to create dimensional and fact tables
- Advanced data analysis to generate insightful aggregate tables
- Data loading into a DuckDB database
- Comprehensive unit tests for data preprocessing and database operations
- Full dockerization of the ETL pipeline for reproducibility and portability

## Prerequisites

- Docker and Docker Compose
- Git

## Quick Start

1. Clone the repository:
git clone https://github.com/Data-Epic/customer_orders_pipeline_peter.git
cd customer_orders_analysis

2. Download the dataset:
- Get the list dataset and information from [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce), unzip the downloaded zip file
- Create a `data/` Place all CSV files in the `data/` directory

3. Build and run the Docker container:
docker-compose up --build

This will:
Build the Docker image based on the provided Dockerfile.
Install necessary dependencies.
Download and unzip the DuckDB CLI.
Unzip the retail dataset.
Run the data ingestion script.
Open the DuckDB CLI.

4. In a new terminal, start the container:
docker start -ai customers-analysis-etl-duckdb-container

5. Query the loaded tables in DuckDB:
```sql
SELECT * FROM fact_table LIMIT 5;
SELECT * FROM top_selling_sellers LIMIT 5;
SELECT * FROM most_used_payment_type LIMIT 5;


Input Data

olist_customers_dataset.csv
olist_order_items_dataset.csv
olist_orders_dataset.csv
olist_products_dataset.csv
olist_sellers_dataset.csv
product_category_name_translation.csv
olist_order_payments_dataset.csv

Output Tables
The pipeline generates the following analytical and aggregate tables in DuckDB:

fact_table
top_selling_sellers
top_selling_product_category
order_status_count
top_average_delivery_time
average_payment_value
loyal_customers
most_used_payment_type

Project Structure
customer_orders_analysis/
├── data/                  # Input CSV files
├── src/                   # Source code files
|── tests/                 # Unit tests
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
├── requirements.txt       # Python dependencies
└── README.md              # This file

Customization
Updating Dependencies:

If you need to update or add dependencies and packages, modify the requirements.txt file and rebuild the Docker image using:

docker compose up --build

Data Ingestion:

Modify the data_ingestion.py script to change the data ingestion process according to your requirements.

Dependency Issues:
Ensure all required dependencies are listed in the requirements.txt file.