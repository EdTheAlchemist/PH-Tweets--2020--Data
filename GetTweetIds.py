from os import listdir
from os.path import isfile, join

import pandas as pd

import csv

# Paths no longer match because this script was used in another
# repository and mainly handled extracting the Tweet IDs
DATA_FOLDER_PATH = "Data/"
TWEETS_FOLDER_PATH = DATA_FOLDER_PATH + "Tweets/"
TWEET_IDS_FOLDER_PATH = DATA_FOLDER_PATH + "TweetIds/"

# Limit of tweets per file. This was mainly set to ensure that
# the Tweet IDs could be uploaded to GitHub
TWEET_IDS_PER_FILE = 1000000

def getAllFilesInDirectory(path=TWEETS_FOLDER_PATH):
	return [f for f in listdir(path) if isfile(join(path, f)) and f[-4:] == ".csv"]

def returnTweetIds(df):
	return df['tweet_id'].to_list()

def loadTweetCsvFile(file_name):
	return pd.read_csv(
		file_name, 
		sep=';',
		lineterminator='\n')

def getAllTweetIds():
	print("Finding all tweet ids...")

	all_file_names = getAllFilesInDirectory()

	tweet_ids = []
	count = 0
	total_files = len(all_file_names)
	for file_name in all_file_names:
		tweet_ids += returnTweetIds(loadTweetCsvFile(TWEETS_FOLDER_PATH + file_name))
		
		count += 1		
		progress = count / total_files * 100
		print("Progress : %.2f%%" % (progress))

	print("Tweet ids loaded into memory.\n")

	return tweet_ids

def saveTweetIdsToCsv(tweet_ids, path=DATA_FOLDER_PATH):
	print("Saving tweets...\n")
	s = pd.Series(tweet_ids)
	s = s.astype("string")

	total_tweet_count = len(s)
	print("Total tweets: %d" % total_tweet_count)

	processed = 0
	while processed < total_tweet_count:
		if processed + TWEET_IDS_PER_FILE >= total_tweet_count:
			series_slice = s[processed : total_tweet_count - 1]
		else:
			series_slice = s[processed : processed + TWEET_IDS_PER_FILE]

		file_number = processed // TWEET_IDS_PER_FILE
		series_slice.to_csv(
			TWEET_IDS_FOLDER_PATH + "tweet_ids_%s.csv" % (file_number),
			sep=',',
			index=False)

		processed += TWEET_IDS_PER_FILE + 1
	
		progress = processed / total_tweet_count * 100
		print("Progress : %.2f%%" % (progress))

	print("All tweet ids have been saved.\n")

def main():
	tweet_ids = getAllTweetIds()
	saveTweetIdsToCsv(tweet_ids)

if __name__ == "__main__":
	main()

