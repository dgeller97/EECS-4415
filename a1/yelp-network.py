# @Author = Jaskaran Garha 214314439 and David Geller 214404255
# @Course: EECS 4415
# @Assignment: 1
import sys
import pandas as pd
import numpy as np


file = sys.argv[1]
chunksize = 10000

# This function maintains dataframe along with the concatonaed [friend][user_id] column
# This way, we can drop the duplicated row if because [friend][user_id] will be the same ex.) 'ababab' 'ababab'
# After that we clean the friend column to exclude whitespace
# then write to file by appending
def checkReverseDup(frame):
	x = pd.concat(chunk_list)
	x = final.drop_duplicates('check_string')
	x = x.drop(['check_string'], axis=1)
	x['friend'] = x['friend'].str.strip()
	with open('yelp-network.txt', 'a') as myfile:
		x.to_csv(myfile, header=False, index = False, sep=' ')

# process it by chunks to aboid memory probelms and shutdown. 10000 chunksize worked best on my PC 
df = pd.DataFrame()
chunk_list = []
for chunk in pd.read_csv(file, chunksize=chunksize):
	df = chunk
	df = df[~df['friends'].str.contains('None')] # Get rows that have friends, exclude others
	new_df = pd.DataFrame(df.friends.str.split(',').tolist(), index=df.user_id).stack() #split friends into 1-1 relationship with user
	new_df = new_df.reset_index([0, 'user_id'])
	new_df = df.merge(new_df)
	new_df = new_df.rename(columns={new_df.columns[-1]: "friend"})
	final = new_df[['user_id', 'friend']] # only get the needed columns
# https://stackoverflow.com/questions/24676705/pandas-drop-duplicates-if-reverse-is-present-between-two-columns/24680568
# Reference used: for removing reverse duplicates
	# making a new column with [friend][user_id]
	final.loc[:, 'check_string'] = final.apply(lambda row: ''.join(sorted([row['user_id'], row['friend']])), axis=1)
	chunk_list.append(final)
	checkReverseDup(chunk_list)
	
	






