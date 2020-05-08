from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka.producer import KafkaProducer

access_token = "1374576763-MJG1pBwCZllwGIoYQWw2XkNIEOtXcUgjp3mvA5t"
access_token_secret =  "zopeHyvzMASoZe6TcOBQjkKJSeozDtqGtwngeNX23hoUz"
consumer_key =  "0NrQhUvVSINGGg5Mi1vYlH4EG"
consumer_secret =  "wu56Si3w9wnW8PAJFUaHtMglh0nJE8o15kkEVJdZA2m48gMzUO"
filter_keywords = ["Coronavirusmexico","covid2019","coronavirususa","covid_19uk","covid-19uk","Briefing_COVID19","coronaapocolypse","coronavirusbrazil","marchapelocorona",
                  "coronavirusbrasil", "coronaday", "coronafest", "coronavirusu", "covid2019pt", "covid19india", "caronavirusindia", "caronavirusoutbreak", "2019nCoV",
                  "codvid_19", "coronavirusinindia", "coronavirusindia", "bayarealockdown", "coronapandemic", "coronaoutbreak", "stayathomechallenge", "stayhomechallenge",
                  "COVID19"]

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send("COVID19", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0,1,0))
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(languages=['en'], track=filter_keywords)
#stream.filter()