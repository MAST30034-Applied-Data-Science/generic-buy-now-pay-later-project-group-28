
from zipfile import ZipFile

# unzip the zip files 
raw_data_path = "../data/tables/"
with ZipFile(raw_data_path+'poa_2011_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)

with ZipFile(raw_data_path+'sa2_2011_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)

with ZipFile(raw_data_path+'poa_sa2_2011.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)

with ZipFile(raw_data_path+'poa_sa2_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extract('CG_POA_2016_SA2_2016.xls',raw_data_path)

with ZipFile(raw_data_path+'sa2_2021_external.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extract('2021 Census GCP Statistical Area 2 for AUS'+ '/2021Census_G01_AUST_SA2.csv',raw_data_path)
   zipObj.extract('2021 Census GCP Statistical Area 2 for AUS'+ '/2021Census_G02_AUST_SA2.csv',raw_data_path)


