import argparse
import os


parser = argparse.ArgumentParser(description='process some files')
parser.add_argument('--merchant')
parser.add_argument('--consumerid')
parser.add_argument('--consumer_info')
parser.add_argument('--transaction')
args = parser.parse_args()


def find_files(filename, search_path):
# Wlaking top-down from the root
   for root, dir, files in os.walk(search_path):
      if filename in files or filename in dir:
         result = os.path.join(root, filename)
         break
   return result

## print(find_files(args.transaction, os.getcwd()))


#import spark
from pyspark.sql import SparkSession
# Create a spark session (which will run spark jobs)
spark = (
    SparkSession.builder.appName("MAST30034 ass2 BNPL group 28")
    .config("spark.sql.repl.eagerEval.enabled", True) 
    .config("spark.sql.parquet.cacheMetadata", "true")
    .config("spark.sql.session.timeZone", "Etc/UTC")
    .getOrCreate()
)

search_path = os.getcwd()

merchants = spark.read.parquet(find_files(args.merchant, search_path))
consumers = spark.read.parquet(find_files(args.consumerid, search_path))
transactions = spark.read.parquet(find_files(args.transaction, search_path))
consumers_csv = spark.read.options(header='True', inferSchema='True', delimiter='|').csv(find_files(args.consumer_info, search_path))

## Hard coding change duplicate column names
consumers_csv = consumers_csv.withColumnRenamed("name", "user_name")
merchants = merchants.withColumnRenamed("name", "merchant_name")

new_transaction = transactions.join(consumers, transactions.user_id == consumers.user_id, "leftouter").drop(consumers.user_id)
new_transaction = new_transaction.join(merchants, new_transaction.merchant_abn == merchants.merchant_abn, "leftouter").drop(merchants.merchant_abn)
new_transaction = new_transaction.join(consumers_csv, new_transaction.consumer_id == consumers_csv.consumer_id, "leftouter").drop(consumers_csv.consumer_id)

new_transaction.write.mode('overwrite').option("header",True).csv(find_files('curated',search_path = search_path)+'/full_data.csv')
new_transaction.write.mode('overwrite').option("header",True).parquet(find_files('curated',search_path = search_path)+'/full_data.parquet')