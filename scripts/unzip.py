import numpy as np
from zipfile import ZipFile
import pandas as pd

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

# define the path to store files
raw_data_path = "../data/tables/"

# unzip the correspondence file
with ZipFile(raw_data_path+'poa_sa2_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extract('CG_POA_2016_SA2_2016.xls',raw_data_path)

# unzip the SA2 external file
with ZipFile(raw_data_path+'sa2_2021_external.zip', 'r') as zipObj:
   # Extract the selected contents of zip file in current directory
   zipObj.extract('2021 Census GCP Statistical Area 2 for AUS'+ '/2021Census_G01_AUST_SA2.csv',raw_data_path)
   zipObj.extract('2021 Census GCP Statistical Area 2 for AUS'+ '/2021Census_G02_AUST_SA2.csv',raw_data_path)

# Below is the process of link Postcode and SA2
poa_sa2_2016 = pd.read_excel(raw_data_path+"CG_POA_2016_SA2_2016.xls",sheet_name = "Table 3", header = 5)

# Drop the null values
poa_sa2_2016.dropna(axis=0, how='any', thresh=None, subset=None, inplace=True)

# Change the format
poa_sa2_2016['SA2_MAINCODE_2016'] = poa_sa2_2016['SA2_MAINCODE_2016'].apply(np.int64)

# Create spark dataframe
poa_sa2_2016 = spark.createDataFrame(poa_sa2_2016)

poa_2016_2021 = spark.read.csv('../data/tables/poa_2016_2021.csv',header=True)
sa2_2016_2021 = spark.read.csv('../data/tables/sa2_2016_2021.csv',header=True)

# Remove useless columns
common_cols = ['RATIO','PERCENTAGE']
poa_sa2_2016 = poa_sa2_2016.drop(*common_cols)
common_cols_new = ['RATIO_FROM_TO', 'NDIV_TO_REGION_QLTY_INDICATOR', 'OVERALL_QUALITY_INDICATOR', 'BMOS_NULL_FLAG', 'INDIV_TO_REGION_QLTY_INDICATOR']
poa_2016_2021 = poa_2016_2021.drop(*common_cols_new)
sa2_2016_2021 = sa2_2016_2021.drop(*common_cols_new)

# Inner join two dataframes to get the postcode which can find at least one SA2 code
table_2016 = poa_2016_2021.join(poa_sa2_2016, poa_2016_2021['POA_CODE_2016'] == poa_sa2_2016['POA_CODE_2016'], how = 'inner')
table_2016 = table_2016.join(sa2_2016_2021, table_2016['SA2_MAINCODE_2016'] == sa2_2016_2021['SA2_MAINCODE_2016'], how = 'inner')
table_2016_new = table_2016['POA_CODE_2021','SA2_CODE_2021','SA2_NAME_2021']
distinct_df_2016 = table_2016_new.distinct()
distinct_df_2016.write.mode('overwrite').option("header",True).csv('../data/tables/postcode_SA2.csv')
