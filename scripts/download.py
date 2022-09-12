


import argparse
import os
from urllib.request import urlretrieve
import pandas as pd
from zipfile import ZipFile

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

parser = argparse.ArgumentParser(description='process some files')
parser.add_argument('--startyear', metavar = "Year", help="Input the year of normal correspondence start year, only support 2011 and 2016")
parser.add_argument('--endyear', metavar = "Year", help="Input the year of normal correspondence end year, only support 2016 and 2021")
parser.add_argument('--corryear', metavar = "Year", help="Input the year of poa and sa2 correspondence, only support 2011")
args = parser.parse_args()


start_year = args.startyear
end_year = args.endyear
corr_year = args.corryear

#define the store path
raw_data_path = "../data/tables/"

# Check whether the specified
# path exists or not
isExist = os.path.exists(raw_data_path)
print(isExist)

if (start_year == "2011") & (end_year == "2016"):
    # Download sa2 zip
    sa2_corr_name = f"sa2_{start_year}_{end_year}.zip"
    urlretrieve(f"https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&cg_sa2_{start_year}_sa2_{end_year}.zip&1270.0.55.001&Data%20Cubes&C9CFBB94B52B200DCA257FED0014C198&0&July%202016&12.07.2016&Latest", raw_data_path + sa2_corr_name)

    # Download poa zip
    poa_corr_name = f"poa_{start_year}_{end_year}.zip"
    urlretrieve(f"https://www.abs.gov.au/ausstats/subscriber.nsf/log?openagent&cg_poa_{start_year}_poa_{end_year}.zip&1270.0.55.003&Data%20Cubes&3399EB98D8341DFFCA25802C0014C3D3&0&July%202016&13.09.2016&Previous", raw_data_path + poa_corr_name)

else:
    #download the poa correspondense 2016 to 2021 data
    poa_1621corr_name = f"poa_{start_year}_{end_year}.csv"
    urlretrieve(f"https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/correspondences/CG_POA_{start_year}_POA_{end_year}.csv", raw_data_path + poa_1621corr_name)

    #download the poa correspondense 2016 to 2021 data
    sa2_1621corr_name = f"sa2_{start_year}_{end_year}.csv"
    urlretrieve(f"https://www.abs.gov.au/statistics/standards/australian-statistical-geography-standard-asgs-edition-3/jul2021-jun2026/access-and-downloads/correspondences/CG_SA2_{start_year}_SA2_{end_year}.csv", raw_data_path + sa2_1621corr_name)

# Download POA to SA2 correspondence zip
poa_sa2_name = "poa_sa2.zip"
urlretrieve(f"https://www.abs.gov.au/AUSSTATS/subscriber.nsf/log?openagent&1270055006_CG_POSTCODE_{corr_year}_SA2_{corr_year}.zip&1270.0.55.006&Data%20Cubes&70A3CE8A2E6F9A6BCA257A29001979B2&0&July%202011&27.06.2012&Latest", raw_data_path + poa_sa2_name)
