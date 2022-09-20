import argparse
import os
import numpy as np
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
import builtins
from cmath import nan


# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("MAST30034 ass2 BNPL group 28")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

## -----------------------------------------------------------------------------------------------------------------------------------------------
## Argparse for selecting files to be preprocessed
## -----------------------------------------------------------------------------------------------------------------------------------------------

## Run following 
# --consumerid consumer_user_details.parquet 
# --merchant tbl_merchants.parquet 
# --consumer_info tbl_consumer.csv 
# --transaction transactions_20210228_20210827_snapshot transactions_20210828_20220227_snapshot 
# --postcode_sa2 postcode_SA2.csv 
# --ex_population 2021Census_G01_AUST_SA2.csv 
# --ex_income 2021Census_G02_AUST_SA2.csv 
# --merchant_fraud merchant_fraud_probability.csv 
# --consumer_fraud consumer_fraud_probability.csv

parser = argparse.ArgumentParser(description='process some files')
parser.add_argument('--merchant', metavar = "file", help="This is the merchant file name you need to input)")
parser.add_argument('--consumerid', metavar = "file", help="This is the consumer parquet file name you need to input")
parser.add_argument('--consumer_info', metavar = "file", help="This is the consumer csv file name you need to input")
parser.add_argument('--transaction', metavar = "file", help="This is the transaction file name you need to input", nargs="+")
parser.add_argument('--postcode_sa2', metavar = "file", help="This is the file containing transformation between postcode and SA2 minicode")
parser.add_argument('--ex_income', metavar = "file", help="This is the Australian average personal income file you need to input")
parser.add_argument('--ex_population', metavar = "file", help="This is the Australian population by SA2 areas file you need to input")
parser.add_argument('--merchant_fraud', metavar = "file", help="This is the fraud merchant file name you need to input)")
parser.add_argument('--consumer_fraud', metavar = "file", help="This is the fraud consumer file name you need to input)")
args = parser.parse_args()


## -----------------------------------------------------------------------------------------------------------------------------------------------
## Read files
## -----------------------------------------------------------------------------------------------------------------------------------------------

# Wlaking top-down from the root to find files
def find_files(filename, search_path):
   for root, dir, files in os.walk(search_path):
      if filename in files or filename in dir:
         result = os.path.join(root, filename)
         break
   return result

search_path = os.getcwd()

merchants = spark.read.parquet(find_files(args.merchant, search_path))
consumers = spark.read.parquet(find_files(args.consumerid, search_path))
consumers_csv = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(find_files(args.consumer_info, search_path))
postcode_SA2 = spark.read.options(header = True).csv(find_files(args.postcode_sa2, search_path))
population = spark.read.options(header = True).csv(find_files(args.ex_population, search_path))
income = spark.read.options(header = True).csv(find_files(args.ex_income, search_path))
merchant_fraud = spark.read.options(header = True).csv(find_files(args.merchant_fraud, search_path))
consumer_fraud = spark.read.options(header = True).csv(find_files(args.consumer_fraud, search_path))

## read all transaction dataset
for i in range(len(args.transaction)):
   if i == 0:
      transactions = spark.read.parquet(find_files(args.transaction[i], search_path))
   else:
      transactions = transactions.unionByName(spark.read.parquet(find_files(args.transaction[i], search_path)), allowMissingColumns = True)


## -----------------------------------------------------------------------------------------------------------------------------------------------
## Deal with given datasets
## -----------------------------------------------------------------------------------------------------------------------------------------------

## Hard coding change duplicate column names
consumers_csv = consumers_csv.withColumnRenamed("name", "user_name")
merchants = merchants.withColumnRenamed("name", "merchant_name")

# Join multiple dataframes into a single
new_transaction = transactions.join(consumers, transactions.user_id == consumers.user_id, "leftouter").drop(consumers.user_id)
new_transaction = new_transaction.join(merchants, new_transaction.merchant_abn == merchants.merchant_abn, "leftouter").drop(merchants.merchant_abn)
new_transaction = new_transaction.join(consumers_csv, new_transaction.consumer_id == consumers_csv.consumer_id, "leftouter").drop(consumers_csv.consumer_id)

## convert dollar value of transaction to float type and only keep two decimal place.
new_transaction = new_transaction.withColumn("dollar_value", new_transaction["dollar_value"].cast('float'))
new_transaction = new_transaction.withColumn("dollar_value", round(new_transaction["dollar_value"], 2))

## save type of order_datatime as DATE
new_transaction = new_transaction.withColumn("order_datetime", to_date(new_transaction["order_datetime"], "yyy-MM-dd"))

## save merchant tags to different columns "field", "renvenue_level" and "take_rate", while "take_rate" is float type
## transform all strings in "field" and "revenue_level" to lowercase
new_transaction = new_transaction.withColumn('tags', expr("substring(tags, 3, length(tags)-4)")) \
    .withColumn('field', split(col("tags"), "\], \[|\), \(").getItem(0)) \
        .withColumn('revenue_level', split(col("tags"), "\], \[|\), \(").getItem(1)) \
            .withColumn('take_rate', split(col("tags"), "\], \[|\), \(").getItem(2)) \
                .withColumn('take_rate', regexp_extract(col("take_rate"), r'(\d+).(\d+)', 0)) \
                    .withColumn("take_rate", col('take_rate').cast(FloatType())) \
                        .withColumn('field', lower(col('field'))) \
                            .withColumn('revenue_level', lower(col('revenue_level'))) \
                                .drop("tags")

## drop unhelpful columns
cols = ['address','gender', 'consumer_id', 'user_name', 'state']
new_transaction = new_transaction.drop(*cols)

## drop rows that have null values
new_transaction = new_transaction.dropna()

## drop transaction that has dollor value less or equal to 0
new_transaction = new_transaction.filter((col('dollar_value') >= 0))

## check order datetime to in the right range
new_transaction = new_transaction.filter((col('order_datetime') >= '2021-02-28') & (col('order_datetime') <= '2022-08-28'))

## check the consistency of postcode
new_transaction = new_transaction.filter(length(col('postcode')) == 4)


## -----------------------------------------------------------------------------------------------------------------------------------------------
## Dealing with external datasets
## -----------------------------------------------------------------------------------------------------------------------------------------------

## Drop unhelpful columns in both external dataset
population = population.select(col('SA2_CODE_2021'), col('Tot_P_P'))
income = income.select(col('SA2_CODE_2021'), col('Median_tot_prsnl_inc_weekly'))

## change data type
population = population.withColumn("Tot_P_P",population.Tot_P_P.cast('int'))
income = income.withColumn('Median_tot_prsnl_inc_weekly', income.Median_tot_prsnl_inc_weekly.cast('float'))

## merge SA2 population (sum of different sa2 areas) with postcode
pos_population = postcode_SA2.join(population, postcode_SA2.SA2_CODE_2021 == population.SA2_CODE_2021, "inner").drop(population.SA2_CODE_2021)
pos_population = pos_population.groupBy('POA_CODE_2021').sum('Tot_P_P')
pos_population = pos_population.withColumnRenamed('sum(Tot_P_P)', 'total_population')

## merge SA2 income (average personal income in different sa2 areas) with postcode
pos_income= postcode_SA2.join(income, postcode_SA2.SA2_CODE_2021 == income.SA2_CODE_2021, "inner").drop(income.SA2_CODE_2021)
pos_income = pos_income.groupBy('POA_CODE_2021').avg('Median_tot_prsnl_inc_weekly')
pos_income = pos_income.withColumn("avg_personal_income_weekly", round(pos_income["avg(Median_tot_prsnl_inc_weekly)"], 1))
pos_income = pos_income.drop('avg(Median_tot_prsnl_inc_weekly)')

## join both income and population with postcode
pos_info = pos_population.join(pos_income, pos_population.POA_CODE_2021 == pos_income.POA_CODE_2021, 'inner').drop(pos_income.POA_CODE_2021)
pos_info = pos_info.select('POA_CODE_2021', 'total_population', 'avg_personal_income_weekly')

## join pos_info with original dataset
new_transaction = new_transaction.join(pos_info, new_transaction.postcode == pos_info.POA_CODE_2021, "leftouter").drop(pos_info.POA_CODE_2021)

round = getattr(builtins, "round")

## Filled by mean
# curated_csv_df1 = curated_csv_new
# income_mean = np.mean(income.select('Median_tot_prsnl_inc_weekly').collect())
# population_mean = np.mean(population.select('Tot_P_P').collect())
# curated_csv_df1 = curated_csv_df1.na.fill({'avg_personal_income_weekly':round(income_mean,1),'total_population':population_mean})

## Filled by median
income_median = np.median(income.select('Median_tot_prsnl_inc_weekly').collect())
population_median = np.median(population.select('Tot_P_P').collect())
new_transaction = new_transaction.na.fill({'avg_personal_income_weekly':round(income_median,1),'total_population':population_median})


## -----------------------------------------------------------------------------------------------------------------------------------------------
## Deal with fraud data
## -----------------------------------------------------------------------------------------------------------------------------------------------

## only keep records that has fraud probability exceed 30 % (which are treated as fraud records)
consumer_fraud = consumer_fraud.withColumn("fraud_probability",consumer_fraud.fraud_probability.cast(FloatType()))
merchant_fraud = merchant_fraud.withColumn("fraud_probability",merchant_fraud.fraud_probability.cast(FloatType()))

consumer_fraud = consumer_fraud.filter(col("fraud_probability") >= 30)
merchant_fraud = merchant_fraud.filter(col("fraud_probability") >= 30)

new_transaction = new_transaction.join(consumer_fraud, (new_transaction.user_id == consumer_fraud.user_id) \
    & (new_transaction.order_datetime == consumer_fraud.order_datetime), "leftouter") \
        .drop(consumer_fraud.user_id) \
            .drop(consumer_fraud.order_datetime) \
                .withColumnRenamed("fraud_probability", "consumer_fraud_prob")

new_transaction = new_transaction.join(merchant_fraud, (new_transaction.merchant_abn == merchant_fraud.merchant_abn) \
    & (new_transaction.order_datetime == merchant_fraud.order_datetime), "leftouter") \
        .drop(merchant_fraud.merchant_abn) \
            .drop(merchant_fraud.order_datetime)\
                .withColumnRenamed('fraud_probability', 'merchant_fraud_prob')

new_transaction = new_transaction.withColumn("is_fraud", \
    when((col('consumer_fraud_prob') != nan) | (col('merchant_fraud_prob') != nan), 'True').otherwise('False')) \
        .drop('consumer_fraud_prob') \
            .drop('merchant_fraud_prob')


## -----------------------------------------------------------------------------------------------------------------------------------------------
## Write files
## -----------------------------------------------------------------------------------------------------------------------------------------------

## Store the Dataframe after joining process in both csv and parquet format
new_transaction.write.mode('overwrite').option("header",True).csv(find_files('curated',search_path = search_path)+'/full_data.csv')
# new_transaction.write.mode('overwrite').option("header",True).parquet(find_files('curated',search_path = search_path)+'/full_data.parquet')