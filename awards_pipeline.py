import numpy as np
import pandas as pd

from xgboost import XGBClassifier

import random

import xgboost
import sklearn

def load_data(\
	pwd,
	awards):
	
	oscars_df = pd.read_csv(pwd+"best_picture_winners_nominees.csv")

	award_dict = {}

	for award in awards:
	    new_key = award.split('.')[0]
	    award_dict[new_key] = pd.read_csv(pwd+award)

	# Combine the output from all awards

	for i, award in enumerate(award_dict.keys()):
	    award_dict[award].rename(columns={"Winner":award},inplace=True)
	    df = award_dict[award][['Movie',award]]
	    
	    if i == 0:
	        merged_df = oscars_df.merge(df,
	                        how="left",
	                        left_on="Title",
	                        right_on="Movie"
	                                   )
	    else:
	        merged_df = merged_df.merge(df,
	                        how="left",
	                        left_on="Title",
	                        right_on="Movie"
	                                   )
	    merged_df = merged_df.drop(columns="Movie")
	    
	merged_df['yr_fix'] = merged_df['Year'].map(lambda x: x.split('(')[0].split('/')[0]).astype(int)

	return merged_df

def categorical_fields(merged_df, awards):
	for award in awards:
		new_key = award.split('.')[0]
		merged_df[new_key] = merged_df[new_key].astype("category")

	merged_df['Winner'] = merged_df['Winner'].astype(int)

	for col in merged_df.select_dtypes("category"):
	    if merged_df[col].cat.categories.dtype == "bool":
	        merged_df[col] = merged_df[col].astype(str).astype("category")

	return merged_df

def main(pwd = '/Users/michaelpeth/Documents/github/pyaward/data/', 
	awards = ['bafta_best_picture.csv', 'dga_best_picture.csv', 'sag_best_picture.csv', 
          'gg_best_comedy.csv', 'gg_best_picture.csv', 'cc_best_picture.csv','pga_best_picture.csv']):
	merged_df = load_data(pwd, awards)
	merged_df_cleaned = categorical_fields(merged_df, awards)

	merged_df_cleaned.to_csv(pwd+'dim_merged_awards_best_picture.csv')
	return

if __name__ == "__main__":
    main()