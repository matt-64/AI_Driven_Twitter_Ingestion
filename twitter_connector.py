import tweepy
import json
import boto3
from datetime import datetime

client = boto3.client('firehose')
twitter_ingest = "Here_is_the_name_of_the_firehose_kinesis"

# query config
query= '@python' #track
max_results = 100
tweet_fields= ['source','public_metrics', 'lang', 'author_id','entities','in_reply_to_user_id', 'created_at']


b_token = "bear_token_value"


#Tweepy objet instance
client_t = tweepy.Client(bearer_token=b_token)

#tweepy request
request = client_t.search_recent_tweets(query=query, max_results=max_results, tweet_fields=tweet_fields)

#Data to Json -> to firehose

# users = {u["id"]: u for u in request.includes['users']}

for items in request.data:
    # if users[items.author_id]:
    #     user = users[items.author_id]
        # name, location = user.username, user.location
    text_json = {
        'source': items.source,
        'reply_count':items.public_metrics["reply_count"],
        'id_str':str(items.id),
        'text':items.text,
        'retweet_count':items.public_metrics["retweet_count"],
        'id':items.id,
        # transform datetime to str for the AI Driven Project 
        'created_at':items.created_at.strftime("%m/%d/%Y, %H:%M:%S"),
        # 'place':user.location,
        'lang':items.lang,
        # # 'user':name,
        'quote_count':items.public_metrics["quote_count"]
        }
    data = json.dumps(text_json)
    response = client.put_record(
        DeliveryStreamName=twitter_ingest,
            Record={
                'Data': data+'\n'
            }
    )
print(f"{data},\n that's the request : {query}")


