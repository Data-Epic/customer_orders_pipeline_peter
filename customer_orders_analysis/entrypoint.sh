#!/bin/bash

# Open DuckDB CLI
/customer_orders_analysis/duckdb customer_orders.duckdb

# Keep the container running
tail -f /dev/null