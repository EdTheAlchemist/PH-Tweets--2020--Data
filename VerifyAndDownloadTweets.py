"""
Main source of info: https://github.com/twitterdev/getting-started-with-the-twitter-api-v2-for-academic-research/blob/main/modules/6a-labs-code-academic-python.md
"""

from Configurations import API_KEY, API_SECRET_KEY, BEARER_TOEKN, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET

from twarc import Twarc2, expansions
from os import listdir
from os.path import isfile, join
from pprint import pprint

import pandas as pd

import json
import csv

TWEET_IDS_FOLDER_PATH = "./TweetIds/"
OUTPUT_FOLDER_PATH = "./Output/"

def loadTweetIds(path=TWEET_IDS_FOLDER_PATH):
    print("Loading tweet IDs...")

    file_names = [f for f in listdir(path) if isfile(join(path, f)) and f[-4:] == ".csv"]
    temp_list = []
    count = 0
    for file_name in file_names:
        temp_list += pd.read_csv(
            path + file_name, 
            index_col=False, 
            header=0, 
            squeeze = True,
            lineterminator='\n').to_list()

        count += 1
        progress = count / len(file_names) * 100
        print("Progress: %.2f%%" % progress)

    print("%d tweet IDs have been loaded into memory." % len(temp_list))
	
    return temp_list

def initializeClient():
	return Twarc2(bearer_token=BEARER_TOEKN)

def saveTweetsToCsv(tweets):
    tweet_df = pd.DataFrame(tweets)
    tweet_df.to_csv(
        OUTPUT_FOLDER_PATH + "tweets.csv",
        sep=',',
        index=False,
        quoting=csv.QUOTE_ALL)

def main():
    tweet_ids = loadTweetIds()

    # Readying the Twitter client to interface
    # Note:
    client = initializeClient()
    lookup = client.tweet_lookup(tweet_ids=tweet_ids[:1000])

    # Here is where all results are placed. Kindly modify this
    # so that you can save the tweets to your own machine. In its
    # current form, this simply takes all tweets and saves some 
    # information to a tweet
    tweets = []
    for page in lookup:
        # This returns a list of tweets
        result = expansions.flatten(page)
        # For each tweet that was found, add it to a list. For
        # this example, I'm only taking the text and when the tweet
        # was created at.
        for tweet in result:
            tweets.append({
                'text': tweet['text'],
                'created_at': tweet['created_at']
                })

    # Save tweets to a CSV file
    saveTweetsToCsv(tweets)

if __name__ == "__main__":
    main()