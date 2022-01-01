"""
Main source of info: https://github.com/twitterdev/getting-started-with-the-twitter-api-v2-for-academic-research/blob/main/modules/6a-labs-code-academic-python.md
"""

from Configurations import API_KEY, API_SECRET_KEY, BEARER_TOEKN, ACCESS_TOKEN_KEY, ACCESS_TOKEN_SECRET

from twarc import Twarc2, expansions
from os.path import isfile, join
from dateutil import parser
from pprint import pprint
from os import listdir

import pandas as pd

import datetime
import json
import pytz
import csv

TWEET_IDS_FOLDER_PATH = "./TweetIds/"
OUTPUT_FOLDER_PATH = "./Output/"
TWEETS_TO_UPDATE = 5000
TWEETS_PER_FILE = 250000

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

def saveTweetsToCsv(tweets, file_name="tweets.csv"):
    print("Saving tweets...")

    tweet_df = pd.DataFrame(tweets)
    file_path = OUTPUT_FOLDER_PATH + file_name

    if isfile(file_path):
        tweet_df.to_csv(
            file_path,
            sep=',',
            index=False,
            mode='a',
            header=False,
            quoting=csv.QUOTE_ALL)
    else:
        tweet_df.to_csv(
            file_path,
            sep=',',
            index=False,
            quoting=csv.QUOTE_ALL)
    
    print("Tweets have been saved.")

def main():
    total_tweet_count = 0
    file_count = 0

    client = initializeClient()
    lookup = client.tweet_lookup(tweet_ids=loadTweetIds()[total_tweet_count:])

    tweets = []
    for page in lookup:
        result = expansions.flatten(page)
        for tweet in result:
            tweets.append({
                'tweet_id': tweet['id'],
                'author_id': tweet['author_id'],
                'created_at_utc+8': datetime.datetime.strftime(
                    parser.parse(tweet['created_at'])
                            .replace(tzinfo=pytz.utc)
                            .astimezone(pytz.timezone('Asia/Manila')), 
                    "%Y-%m-%d %H:%M:%S"),
                'lang': tweet['lang'],
                'text': tweet['text']
                })

            total_tweet_count += 1
            if total_tweet_count % TWEETS_TO_UPDATE == 0:
                saveTweetsToCsv(tweets, "tweets_" + str(file_count) + ".csv")
                tweets = []
                if total_tweet_count % TWEETS_PER_FILE == 0:
                    file_count += 1

if __name__ == "__main__":
    main()