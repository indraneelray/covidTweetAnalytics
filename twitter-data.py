from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka.producer import KafkaProducer
import json

access_token = "-"
access_token_secret =  ""
consumer_key =  ""
consumer_secret =  ""
filter_keywords = ["Coronavirusmexico","covid2019","coronavirususa","covid_19uk","covid-19uk","Briefing_COVID19","coronaapocolypse","coronavirusbrazil","marchapelocorona",
                  "coronavirusbrasil", "coronaday", "coronafest", "coronavirusu", "covid2019pt", "covid19india", "caronavirusindia", "caronavirusoutbreak", "2019nCoV",
                  "codvid_19", "coronavirusinindia", "coronavirusindia", "bayarealockdown", "coronapandemic", "coronaoutbreak", "stayathomechallenge", "stayhomechallenge",
                  "COVID19"]

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_data = json.loads(data)
        new_data = format_twitter_data(json_data)
        new_str = (json.dumps(new_data)).replace("\'", "\"")
        #print (new_str)
        producer.send("COVID19", (new_str).encode('utf-8'))
        #print (data)
        return True
    def on_error(self, status):
        print (status)

def format_twitter_data(json_data):
    new_data = {
        "created_at" : json_data["created_at"],
        "id" : json_data["id"],
        "text" : json_data["text"],
        "coordinates" : json_data["coordinates"],
        "place" : json_data["place"],
        "reply_count" : json_data["reply_count"],
        "retweet_count" : json_data["retweet_count"],
        "entities" : json_data["entities"],
        "favorited" : json_data["favorited"],
        "retweeted" : json_data["retweeted"],
        "lang" : json_data["lang"]
    }
    if "user" in json_data:
        new_data["user"] = {
            "location" : json_data["user"]["location"],
            "geo-enabled" : json_data["user"]["geo_enabled"],
            "verified" : json_data["user"]["verified"],
            "description" : json_data["user"]["description"]
        }
    
    if "retweeted_status" in json_data:
        new_data["retweeted_status"] = {
            "text" : json_data["retweeted_status"]["text"]
        }
        if "user" in json_data["retweeted_status"]:
            new_data["retweeted_status"]["user"] = {
                "id" : json_data["retweeted_status"]["user"]["id"],
                "location" : json_data["retweeted_status"]["user"]["location"]
            }
        if "extended_tweet" in json_data["retweeted_status"]:
             new_data["retweeted_status"]["extended_tweet"] = {
                 "full_text" : json_data["retweeted_status"]["extended_tweet"]["full_text"]
             }
    return new_data

producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(languages=['en'], track=filter_keywords)
#stream.filter()
