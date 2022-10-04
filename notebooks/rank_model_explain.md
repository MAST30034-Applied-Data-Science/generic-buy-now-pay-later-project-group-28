# Ranking Model
- rank_algorithm(merchant_info, feature_name, score_criteria, remove_rate, top_n)

# Model Target
- The goal of the model is to recommend to BNPL company the top N cooperative merchants that are in the long-term interest according to some specific characteristics.

# Model Arguments 
- merchant_info (a Pandas.DataFrame): should contain business and customer information for all merchants, each type of information is stored in a column
- feature_name (a list) : is the list of column names that should be considered in the ranking model.
- score_creteria (a dict): is a dictionary of dictionary that stroe different weighting strategies of different column features.
- remove rate (a float between 0.0 and 1.0) is the dorp rate of the most unwanted merchants at each run of the algorithm. Note, (# of removed merchants = remove_rate * # of current merchants)
- top_n (a positive integer) can be recognized as a stoppong criteriion of the ranking algorithm to give at most n besst merchants.

# Features of a Merchant
- transaction_count: number of transcations made in a specified period.
- take_rate: the fee charged by the BNPL firm to a merchant on a transaction. That is, for each transaction made, a certain percentage is taken by the BNPL firm.
- revenue_level: (a, b, c, d, e) represents the level of revenue bands (unknown to groups). 'a' denotes the smallest band whilst 'e' denotes the highest revenue band.
- total_revenue: the total revenue made by a merchant in a specified period.
- mean_consumer_income: the mean weekly income of each merchant's consumers. (used to represents the puchasing power of merchants' target audience)
- fraud_count: the number of transactions that are recongnized as fraud.
- main_business_area_popu: a sum of the number of consumers in the top five postcode areas corresponding to each merchant that has most users within these areas.

# Model Theory (Implementation of Jeremy-Rudy Algorithm)
- Step 1: Setting arguments for the ranking system, especially (score_criteria, remove_rate, top_n). Note, score_criteria should be set very carefully otherwise the model could be meaningless.
- Step 2: Converting all entries of each numeric column into categorical levels (a, b, c, d, e) according to the (80%, 60%, 40%, 20%) quantiles of the current data.
- Step 3: Mark all entries of each numeric column according to the score_criteria given, and then sum the column marks of each merchant (store mark in a new column 'score').
- Step 4: Sort all merchants by their mark (descending order) and drop {len(merchant_info) * remove_rate} merchants form tail. 
- Step 5: Remove the column 'score' and use the merchants left to implement this algorithm again. (Stop until the number  merchants is going to go below 100 after the next run)

# Model Explanation:
- Basic Consideration:
	- We give each merchant a rating level (a, b, c, d, e) for each feature, while the rating process is achieved by finding the 80th, 60th, 40th and 20th percentiles of all the data for each feature.
	- Each level could have different marks assigned in different feature, while in all features, except for revenue level, the level e is the worst level.
	- The algorithm is expected to run multiple times, each time a specified percentage of tail (or unwanted) merchants are removed. Then the remaining merchants with score to be reset are prepared for the next run, until we finally obtain the top n merchants.
	- In general, at each run, there will always groups with lower total marks since the Mark Algorithm is based on the current merchants, and we update the remaining merchants at each run.
	- Therefore, for example, at each run, those merchant with all features (except revenue level) to be level 'e' are very likely to be removed as their total marks could be very low and be considered within the drop list
	- In conclusion, for each time we run this algorithm, we don't try to figure out the 'best merchants' as the result could be very unreliable, instead, we aim to find merchants that are cosidered to be the weakest and remove them to ensure accuracy.

- score_criteria:
	- This model will recommend the top n merchants according to our business goal by setting the 'score_criteria' properly.
	- By default, the model will weight all features equally, which means, for instance, the 'transcation_count', 'take_rate' and 'total_revenue' are equally important as criteria for choosing best merchants for BNPL company.
	- However, a 'fair model' is not always a good choice. Actually, a BNPL company may focus more on a merchant's total revenue and take rate, rather than its transaction count as BNPL company can earn more when both the former two terms are high.
	- Therefore, the BNPL company may want to weight more on company with higher 'total_revenue' and 'take_rate', which can be achieved by manually increse the 'score_criteria' for the two terms.