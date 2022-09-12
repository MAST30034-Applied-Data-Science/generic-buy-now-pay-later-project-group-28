
from zipfile import ZipFile

# unzip the zip files 
raw_data_path = "../data/tables/"
with ZipFile(raw_data_path+'poa_2011_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)

with ZipFile(raw_data_path+'sa2_2011_2016.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)

with ZipFile(raw_data_path+'poa_sa2.zip', 'r') as zipObj:
   # Extract all the contents of zip file in current directory
   zipObj.extractall(raw_data_path)