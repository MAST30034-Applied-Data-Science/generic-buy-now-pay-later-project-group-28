# Generic Buy Now, Pay Later Project
This project is building a ranking system to select the best merchants
External packages are needed, please check requirement to ensure you have them downloaded

## Procedure
### 1: Run download.py
Make sure you are in the scripts directory and run the following code: python3 download.py --startyear 2016 --endyear 2021 --corryear 2016

This script will download the required dataset for this ranking system

### 2: Run the unzip.py
In the scripts folder, run the following code: python3 unzip.py

This will unzip the zip file downloading from the external website

### 3: Run the read_data.py
In the main folder, run the following code: python3 scripts/read_data.py --consumerid consumer_user_details.parquet --merchant tbl_merchants.parquet --consumer_info tbl_consumer.csv --transaction transactions_20210228_20210827_snapshot transactions_20210828_20220227_snapshot transactions_20220228_20220828_snapshot --postcode_sa2 postcode_SA2.csv --ex_income 2021Census_G02_AUST_SA2.csv --ex_population 2021Census_G01_AUST_SA2.csv --merchant_fraud merchant_fraud_probability.csv  --consumer_fraud consumer_fraud_probability.csv

This will generate the actual datasets for ranking and stored in local

### 4: Run the ranking.py
In the main folder, run the following code: python3 scripts/ranking.py --merchant_info merchant_info.csv

This will generate 4 CSVs, including the top 100 ranking for all merchants, and three top 10 rankings for merchants in 3 different segments.
