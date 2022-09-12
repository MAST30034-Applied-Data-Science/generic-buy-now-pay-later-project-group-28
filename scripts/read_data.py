import argparse
import os
import pyspark.sql.functions as func
from pyspark.sql.types import FloatType
from pyspark.sql import SparkSession
# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("MAST30034 ass2 BNPL group 28")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)


parser = argparse.ArgumentParser(description='process some files')
parser.add_argument('--merchant', metavar = "file", help="This is the merchant file name you need to input)")
parser.add_argument('--consumerid', metavar = "file", help="This is the consumer parquet file name you need to input")
parser.add_argument('--consumer_info', metavar = "file", help="This is the consumer csv file name you need to input")
parser.add_argument('--transaction', metavar = "file", help="This is the transaction file name you need to input", nargs="+")
args = parser.parse_args()


def find_files(filename, search_path):
# Wlaking top-down from the root
   for root, dir, files in os.walk(search_path):
      if filename in files or filename in dir:
         result = os.path.join(root, filename)
         break
   return result

## print(find_files(args.transaction, os.getcwd()))


search_path = os.getcwd()

merchants = spark.read.parquet(find_files(args.merchant, search_path))
consumers = spark.read.parquet(find_files(args.consumerid, search_path))
consumers_csv = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(find_files(args.consumer_info, search_path))

for i in range(len(args.transaction)):
   if i == 0:
      transactions = spark.read.parquet(find_files(args.transaction[i], search_path))
   else:
      transactions = transactions.unionByName(spark.read.parquet(find_files(args.transaction[i], search_path)), allowMissingColumns = True)

## Hard coding change duplicate column names
consumers_csv = consumers_csv.withColumnRenamed("name", "user_name")
merchants = merchants.withColumnRenamed("name", "merchant_name")

# Join multiple dataframes into a single
new_transaction = transactions.join(consumers, transactions.user_id == consumers.user_id, "leftouter").drop(consumers.user_id)
new_transaction = new_transaction.join(merchants, new_transaction.merchant_abn == merchants.merchant_abn, "leftouter").drop(merchants.merchant_abn)
new_transaction = new_transaction.join(consumers_csv, new_transaction.consumer_id == consumers_csv.consumer_id, "leftouter").drop(consumers_csv.consumer_id)

## convert dollar value of transaction to float type and only keep two decimal place.
new_transaction = new_transaction.withColumn("dollar_value", new_transaction["dollar_value"].cast('float'))
new_transaction = new_transaction.withColumn("dollar_value", func.round(new_transaction["dollar_value"], 2))

## save type of order_datatime as DATE
new_transaction = new_transaction.withColumn("order_datetime", func.to_date(new_transaction["order_datetime"], "yyy-MM-dd"))

## save merchant tags to different columns "field", "renvenue_level" and "take_rate", while "take_rate" is float type
## transform all strings in "field" and "revenue_level" to lowercase
new_transaction = new_transaction.withColumn('tags', func.expr("substring(tags, 3, length(tags)-4)")) \
    .withColumn('field', func.split(func.col("tags"), "\], \[|\), \(").getItem(0)) \
        .withColumn('revenue_level', func.split(func.col("tags"), "\], \[|\), \(").getItem(1)) \
            .withColumn('take_rate', func.split(func.col("tags"), "\], \[|\), \(").getItem(2)) \
                .withColumn('take_rate', func.regexp_extract(func.col("take_rate"), r'(\d+).(\d+)', 0)) \
                    .withColumn("take_rate", func.col('take_rate').cast(FloatType())) \
                        .withColumn('field', func.lower(func.col('field'))) \
                            .withColumn('revenue_level', func.lower(func.col('revenue_level'))) \
                                .drop("tags")


# Store the Dataframe after joining process in both csv and parquet format
new_transaction.write.mode('overwrite').option("header",True).csv(find_files('curated',search_path = search_path)+'/full_data.csv')
## new_transaction.write.mode('overwrite').option("header",True).parquet(find_files('curated',search_path = search_path)+'/full_data.parquet')