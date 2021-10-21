from __future__ import print_function, division

from requests.exceptions import ChunkedEncodingError
from pprint import pprint
from time import sleep

# Please note that Configurations was my own file that wasn't
# push to the repository so that the important tokens wouldn't
# be published.
import Configurations as cfg
import pandas as pd

import traceback
import datetime
import twitter
import os

TWEET_FILE_LIMIT = 200000
TWEET_QUEUE_LIMIT = 200
USER_FILE_LIMIT = 50000

# Keys used for the Twitter client
CONSUMER_KEY = cfg.CONSUMER_KEY
CONSUMER_SECRET = cfg.CONSUMER_SECRET
ACCESS_TOKEN_KEY = cfg.ACCESS_TOKEN_KEY
ACCESS_TOKEN_SECRET = cfg.ACCESS_TOKEN_SECRET

# File paths
TWEETS_FP = cfg.TWEETS_FP 
USERS_FP = cfg.USERS_FP
QUOTED_TWEETS_FP = cfg.QUOTED_TWEETS_FP

# Bounding box of the Philippines
PH_BOX = ["117.17427453", "5.58100332277", "126.537423944", "18.5052273625"]

# Returns the mid point between a given bounding box
# Was used to reduece the need to save 4 separate coordinates
def compute_mid_point(box):
	x1 = box[0][0][0]
	y1 = box[0][0][1]
	x2 = box[0][2][0]
	y2 = box[0][2][1]

	x = x1 + ((x2 - x1) / 2)
	y = y1 + ((y2 - y1) / 2)

	return "%f, %f" % (x, y)

api = twitter.Api(
	consumer_key=CONSUMER_KEY,
	consumer_secret=CONSUMER_SECRET,
	access_token_key=ACCESS_TOKEN_KEY,
	access_token_secret=ACCESS_TOKEN_SECRET,
	sleep_on_rate_limit=True)

while True:
	# Each list here is a temp storage for items to be saved in the CSv
	tweet_list = []
	user_list = []
	quoted_tweet_list = []
	update_count = 0

	# File counts based on list of files found per directory; -1 for .dstore I can't get rid off
	tweet_file_count = len(os.listdir(TWEETS_FP)) - 1
	quoted_tweet_file_count = len(os.listdir(QUOTED_TWEETS_FP)) - 1
	user_file_count = len(os.listdir(USERS_FP)) - 1

	print("----------\nFile Counts\n----------")
	print("Tweets: %d" % tweet_file_count)
	print("Quoted Tweets: %d" % quoted_tweet_file_count)
	print("Users: %d" % user_file_count)

	print("Loading in latest files...")

	tweet_df = pd.read_csv(TWEETS_FP + "Tweets %d.csv" % tweet_file_count, sep=';', lineterminator='\n', index_col=0)
	quoted_tweet_df = pd.read_csv(QUOTED_TWEETS_FP + "Quoted Tweets %d.csv" % quoted_tweet_file_count, sep=';', index_col=0)
	user_df = pd.read_csv(USERS_FP + "Users %d.csv" % user_file_count, sep=';', index_col=0)

	print("----------\nCurrent Element Counts\n----------")
	print("Tweets: %d (%s)" % (len(tweet_df.index), "Tweets %d.csv" % tweet_file_count))
	print("Quoted Tweets: %d (%s)" % (len(quoted_tweet_df.index), "Quoted Tweets %d.csv" % quoted_tweet_file_count))
	print("Users: %d (%s)" % (len(user_df.index), "Users %d.csv" % user_file_count))

	try:
		print("Starting up Twitter stream...")
		previous = datetime.datetime.now()
		for tweet in api.GetStreamFilter(locations=PH_BOX):
			# Handle the basic information of a tweet
			temp = {
				'tweet_id': tweet['id'],
				'created_at': tweet['created_at'],
				'text': tweet['extended_tweet']['full_text'] if tweet['truncated'] else tweet['text'],
				'lang': tweet['lang'],
				'media': tweet['extended_entities']['media'][0]['type'] if 'extended_entities' in tweet else None,
				'in_reply_to_status_id': tweet['in_reply_to_status_id'],
				'in_reply_to_user_id': tweet['in_reply_to_user_id'],

				'user_id': tweet['user']['id'],
				'quoted_tweet_id': None if not(tweet['is_quote_status']) else tweet['quoted_status']['id'] if 'quoted_status' in tweet else "__NA__",
			}
			if tweet['place'] != None:
				if tweet['place']['bounding_box'] != None:
					temp['place_coordinates'] = compute_mid_point(tweet['place']['bounding_box']['coordinates'])
					temp['place_full_name'] = tweet['place']['full_name']
				else:
					temp['place_coordinates'] = "%f, %f" % (tweet['coordinates']['coordinates'][0], tweet['coordinates']['coordinates'][1])
					temp['place_full_name'] = None
			elif tweet['coordinates'] != None:
				temp['place_coordinates'] = "%f, %f" % (tweet['coordinates']['coordinates'][0], tweet['coordinates']['coordinates'][1])
				temp['place_full_name'] = None

			tweet_list.append(temp)
			##########

			# Handle the basic information of a user of a tweet
			temp = {
				'user_id': tweet['user']['id'],
				'created_at': tweet['user']['created_at'],
				'screen_name': tweet['user']['screen_name'],
				'followers_count': tweet['user']['followers_count'],
			}

			user_list.append(temp)
			##########

			# If a quote tweet exists, determine if the tweet is quoting another tweet
			if tweet['is_quote_status']:
				if 'quoted_status' in tweet:
					temp = {
						'quoted_tweet_id': tweet['quoted_status']['id'],
						'created_at': tweet['quoted_status']['created_at'],
						'text': tweet['quoted_status']['extended_tweet']['full_text'] if tweet['quoted_status']['truncated'] else tweet['quoted_status']['text'],
						'quote_count': tweet['quoted_status']['quote_count'],
						'reply_count': tweet['quoted_status']['reply_count'],
						'favorite_count': tweet['quoted_status']['favorite_count'],
						'in_reply_to_status_id': tweet['quoted_status']['in_reply_to_status_id'],
						'in_reply_to_user_id': tweet['quoted_status']['in_reply_to_user_id'],

						'user_id': tweet['quoted_status']['user']['id'],
					}
					if 'extended_entities' in tweet['quoted_status']:
						temp['media'] = tweet['quoted_status']['extended_entities']['media'][0]['type']
					elif 'extended_tweet' in tweet['quoted_status'] and 'extended_entities' in tweet['quoted_status']['extended_tweet']:
						temp['media'] = tweet['quoted_status']['extended_tweet']['extended_entities']['media'][0]['type'],
					else:
						temp['media'] = None
					quoted_tweet_list.append(temp)
					##########

					temp = {
						'user_id': tweet['quoted_status']['user']['id'],
						'created_at': tweet['quoted_status']['user']['created_at'],
						'screen_name': tweet['quoted_status']['user']['screen_name'],
						'followers_count': tweet['quoted_status']['user']['followers_count'],
					}

				else:
					temp = {
						'quoted_tweet_id': tweet['quoted_status_id'] if 'quoted_status_id' in tweet else "__NA__",
						'created_at': "__NA__",
						'text': "__NA__",
						'quote_count': "__NA__",
						'reply_count': "__NA__",
						'favorite_count': "__NA__",
						'media': "__NA__",
						'in_reply_to_status_id': "__NA__",
						'in_reply_to_user_id': "__NA__",

						'user_id': "__NA__",
					}
					quoted_tweet_list.append(temp)
					##########
				
					temp = {
						'user_id': tweet['quoted_status_id'] if 'quoted_status_id' in tweet else "__NA__",
						'created_at': "__NA__",
						'screen_name': "__NA__",
						'followers_count': "__NA__",
					}

				user_list.append(temp)
				##########

			if len(tweet_list) % TWEET_QUEUE_LIMIT == 0:
				print("Merging collected data with current data...")
				update_count = update_count + 1

				temp_df = pd.DataFrame(tweet_list)
				temp_df.set_index("tweet_id", inplace=True)
				tweet_df = pd.concat([tweet_df, temp_df])

				temp_df = pd.DataFrame(quoted_tweet_list)
				temp_df.set_index("quoted_tweet_id", inplace=True)
				quoted_tweet_df = pd.concat([quoted_tweet_df, temp_df])

				temp_df = pd.DataFrame(user_list)
				temp_df.set_index("user_id", inplace=True)
				user_df = pd.concat([user_df, temp_df])
				user_df = user_df[~user_df.index.duplicated(keep='last')]

				tweet_list = []
				quoted_tweet_list = []
				user_list = []

				print("Writing data to file... (WARNING!)")
				user_df.to_csv(USERS_FP + "Users %d.csv" % user_file_count, sep=';', encoding='utf-8')
				tweet_df.to_csv(TWEETS_FP + "Tweets %d.csv" % tweet_file_count, sep=';', encoding='utf-8')
				quoted_tweet_df.to_csv(QUOTED_TWEETS_FP + "Quoted Tweets %d.csv" % quoted_tweet_file_count, sep=';', encoding='utf-8')
				print("Writing completed.\n")
				
				if update_count % 4 == 0:
					current = datetime.datetime.now()
					print("\n==UPDATE as of %s==" % current)
					print("Tweets: %d" % len(tweet_df.index))
					print("Quoted Tweets: %d" % len(quoted_tweet_df.index))	
					print("Users: %d" % len(user_df.index))	
					print("-----%f minutes since last-----\n" % ((current - previous).seconds / 60))
					previous = current

				if len(tweet_df.index) > TWEET_FILE_LIMIT:
					tweet_df.drop(tweet_df.index, inplace=True)
					tweet_file_count = tweet_file_count + 1

				if len(quoted_tweet_df.index) > TWEET_FILE_LIMIT:
					quoted_tweet_df.drop(quoted_tweet_df.index, inplace=True)
					quoted_tweet_file_count = quoted_tweet_file_count + 1

				if len(user_df.index) > USER_FILE_LIMIT:
					user_df.drop(user_df.index, inplace=True)
					user_file_count = user_file_count + 1

	except ChunkedEncodingError:
		print("=====\nWhoops... Chucked Enconding Error. \nRestarting script...\n.\n.\n.")
		continue

	except Exception:
		traceback.print_exc()
		pprint(tweet)
