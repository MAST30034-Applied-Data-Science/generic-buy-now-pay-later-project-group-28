import pandas as pd
import argparse
import os

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
parser.add_argument('--merchant_info', metavar = "file", help="This is the merchant file name you need to rank)")
args = parser.parse_args()

# Wlaking top-down from the root to find files
def find_files(filename, search_path):
   for root, dir, files in os.walk(search_path):
      if filename in files or filename in dir:
         result = os.path.join(root, filename)
         break
   return result

search_path = os.getcwd()

## read the merchant data
merchant_info = spark.read.options(header = True).csv(find_files(args.merchant_info, search_path))
merchant_tents = merchant_info.where(merchant_info.field == "tent and awning shops")
merchant_jewl = merchant_info.where(merchant_info.field == "jewelry, watch, clock, and silverware shops")
merchant_shoe = merchant_info.where(merchant_info.field == "shoe shops")

# Transform to pandas dataframe
merchant_info = merchant_info.toPandas()
merchant_tents = merchant_tents.toPandas()
merchant_jewl = merchant_jewl.toPandas()
merchant_shoe = merchant_shoe.toPandas()

## convert dataset columns into fesible types
merchant_info[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']] \
    = merchant_info[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']].apply(pd.to_numeric)

merchant_tents[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']] \
    = merchant_tents[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']].apply(pd.to_numeric)

merchant_jewl[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']] \
    = merchant_jewl[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']].apply(pd.to_numeric)

merchant_shoe[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']] \
    = merchant_shoe[['transaction_count', 'take_rate', 'total_revenue', 'mean_consumer_income', 'fraud_count', 'main_business_area_popu']].apply(pd.to_numeric)


## Manually set the score criteria, could be changed later.
score_criteria = { \
    'transaction_count': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
        'take_rate': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
            'revenue_level': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                'total_revenue': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                    'mean_consumer_income': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                        'fraud_count': {'a':0, 'b': 0, 'c': 0, 'd': 0, 'e': 0}, \
                            'main_business_area_popu': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}}

## store all numeric features into list for use in both Algorithms
featrue_name = list(merchant_info.keys())
featrue_name.remove('merchant_abn')
featrue_name.remove('field')


## Function used to assign continuous features into level (a,b,c,d,e) based on current merchant infomation
## merchant_info (a pd.DataFrame) is the current merchant information that are considered in our ranking system
## feature (a string) is the numeric feature we are working on
## score_criteria (a dict of dict) is the score criteria we are using for marking different features.
def mark_algorithm(merchant_info, feature, score_criteria):

    label_list = []
    score_list = []

    ## in each run of the ranking system, each numeric column is allocated with a level in (a,b,c,d,e)
    ## according to the overall quantile statistic of the feature column we are working on
    ab = merchant_info[feature].quantile(0.8)
    bc = merchant_info[feature].quantile(0.6)
    cd = merchant_info[feature].quantile(0.4)
    de = merchant_info[feature].quantile(0.2)
    
    for i in range(len(merchant_info)):
        if merchant_info.loc[i, feature] >= ab:
            label_list.append('a')
        elif (merchant_info.loc[i, feature] >= bc) & (merchant_info.loc[i, feature] < ab):
            label_list.append('b')
        elif (merchant_info.loc[i, feature] >= cd) & (merchant_info.loc[i, feature] < bc):
            label_list.append('c')
        elif (merchant_info.loc[i, feature] >= de) & (merchant_info.loc[i, feature] < cd):
            label_list.append('d')
        elif merchant_info.loc[i, feature] < de:
            label_list.append('e')

    ## mark each entry of this feature column according to the score_criteria given.
    for j in label_list:
        score_list.append(score_criteria[feature][j])
    return score_list


## Function used to rank mechant iteratively and then find the top 10 merchants to collaborate.
## merchant_info (a pd.DataFrame) is the current merchant information that are considered in our ranking system
## feature (a string) is the numeric feature we are working on
## score_criteria (a dict of dict) is the score criteria we are using for marking different features.
## remove_rate (a positive float {0,1}) is the rate to remove merchants classified as poor choice
## top_n (a positive integer) is the number of merchant to be recommended.

## Note: the lower the remove_rate, the higher the accuracy but also the longer the waiting time. (too low may cause overfitting)
def rank_algorithm(merchant_info, feature_name, score_criteria, remove_rate, top_n):
    # Stop when we find the asked number of merchants as recommendation
    while (len(merchant_info) > top_n) & (len(merchant_info) * (1-remove_rate) > top_n):
        # In each run, set the score of all merchant to zero as a fresh analysis
        merchant_info['score'] = 0

        # The main part of our rank system to mark and sum scores of all feature columns 
        for feature in feature_name:
            if feature == 'revenue_level':
                for i in range(len(merchant_info)):
                    merchant_info.loc[i, 'score'] += score_criteria[feature][merchant_info.loc[i, feature]]
            else:
                # use of mark algorthm
                merchant_info['score'] += mark_algorithm(merchant_info, feature, score_criteria)

        # sort all merchants by their total score        
        merchant_info = merchant_info.sort_values(by=['score'], ascending=False).reset_index(drop=True)
        # remove a percentage number of merchants having the lowest score (according to remove_rate given)
        merchant_info = merchant_info.drop(merchant_info.tail(int(len(merchant_info) * remove_rate)).index)
        # remove the score column after sorting and dropping merchants. (keep data integrity for next run of this algorithm)
        merchant_info = merchant_info.drop(['score'], axis=1)
        
        # if the stop criteria is not met, keep running to remove merchants that are less likely to be beneficial.
        return rank_algorithm(merchant_info, feature_name, score_criteria, remove_rate, top_n)
    
    # Show the top n merchants once stop criteria are met.
    return merchant_info.head(top_n)

# ---------------------------------------------------------------------------------------------------------------------------------------------------

top_n_merchant = rank_algorithm(merchant_info, featrue_name, score_criteria, 0.1, 100)
top_n_merchant.to_csv((find_files('curated',search_path = search_path)+'/top100.csv'))

# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Higher priority means the algorithm tends to select the merchant that can make the BNPL company earn more on each transaction
score_criteria_shoe = { \
    'transaction_count': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
        'take_rate': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
            'revenue_level': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                'total_revenue': {'a': 4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                    'mean_consumer_income': {'a':4, 'b': 3.5, 'c': 3, 'd': 2.5, 'e': 2}, \
                        'fraud_count': {'a':0, 'b': 0, 'c': 0, 'd': 0, 'e': 0}, \
                            'main_business_area_popu': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}}

top_10_merchant_shoe_designed = rank_algorithm(merchant_shoe, featrue_name, score_criteria_shoe, 0.2, 10)
top_10_merchant_shoe_designed.to_csv(find_files('curated',search_path = search_path)+'/top10_shoes.csv')

# ---------------------------------------------------------------------------------------------------------------------------------------------------

# Higher priority means the algorithm tends to select the merchant that can make the BNPL company earn more on each transaction
score_criteria_tents = { \
    'transaction_count': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
        'take_rate': {'a':40, 'b': 30, 'c': 20, 'd': 10, 'e': 0}, \
            'revenue_level': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                'total_revenue': {'a': 8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
                    'mean_consumer_income': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                        'fraud_count': {'a':0, 'b': 0, 'c': 0, 'd': 0, 'e': 0}, \
                            'main_business_area_popu': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}}

top_10_merchant_tents_designed = rank_algorithm(merchant_tents, featrue_name, score_criteria_tents, 0.2, 10)
top_10_merchant_tents_designed.to_csv(find_files('curated',search_path = search_path)+'/top10_tents.csv')

# ---------------------------------------------------------------------------------------------------------------------------------------------------

score_criteria_jewlery = { \
    'transaction_count': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
        'take_rate': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
            'revenue_level': {'a':4, 'b': 3, 'c': 2, 'd': 1, 'e': 0}, \
                'total_revenue': {'a': 6, 'b': 4.5, 'c': 3, 'd': 1.5, 'e': 0}, \
                    'mean_consumer_income': {'a':8, 'b': 6, 'c': 4, 'd': 2, 'e': 0}, \
                        'fraud_count': {'a':0, 'b': 0, 'c': 0, 'd': 0, 'e': 0}, \
                            'main_business_area_popu': {'a':4, 'b': 3.5, 'c': 3, 'd': 2.5, 'e': 2}}

top_10_merchant_jewlery_designed = rank_algorithm(merchant_jewl, featrue_name, score_criteria_jewlery, 0.2, 10)
top_10_merchant_jewlery_designed.to_csv(find_files('curated',search_path = search_path)+'/top10_jewlery.csv')