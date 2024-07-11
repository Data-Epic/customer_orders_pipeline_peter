FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /customer_orders_analysis

#Install DuckDB
RUN apt-get update && apt-get install -y wget
RUN apt-get update && apt-get install -y unzip
RUN wget https://github.com/duckdb/duckdb/releases/download/v0.8.1/duckdb_cli-linux-amd64.zip
RUN unzip duckdb_cli-linux-amd64.zip
RUN chmod +x duckdb

#install dependencies
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh


# Copy the entire src directory, which includes main.py
COPY src /customer_orders_analysis/src
COPY data /customer_orders_analysis/data

RUN python src/main.py
#copy the entire tests directory, which includes the test files
COPY tests /customer_orders_analysis/tests

# Set the working directory to where the test files are located
WORKDIR /customer_orders_analysis/tests

# Run the tests
RUN python -m unittest -v

WORKDIR /customer_orders_analysis















