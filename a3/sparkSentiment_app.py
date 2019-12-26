"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit sparkSentiment_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""

# @Authors = Jaskaran Garha 214314439 and David Geller 214404255
# @Course: EECS 4415
# @Assignment: 3

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA 
import sys
import requests
import os

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 5 seconds
ssc = StreamingContext(sc, 5)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)

# dictionary holding all hashtags (values) for each topic (keys)
# competing topics: Republicans vs Democrats, Apple vs Samsung vs Google
# Tesla topic just for more variety and trending tags right now
dic = {'Republicans': ['#trump', '#gop', '#impeachement', '#impeachmenthearing', '#republicans'],
       'Democrats': ['#clinton', '#obama', '#democraticdebate', '#democraticparty', '#democrats'],
       'Apple': ['#mac', '#iphone', '#iphone11', '#airpods', '#ios', '#apple'],
       'Samsung': ['#galaxy', '#galaxys10', '#galaxys11', '#galaxynote10', '#samsung'],
       'Google': ['#pixel', '#pixel4', '#android', '#pixelbuds', '#nest', '#mini'],
       'Tesla': ['#cybertruck', '#elon', '#musk', '#elonmusk', '#tesla']}

# function used to filter out tweets in data that contain the tags in the dictionary
# returns boolean (true if tag in tweet is in dictionary, false otherwise)
def tag_filter(line):
    res = False
    for word in line.split(" "):
        for tags in dic.values():
            if word.lower() in tags:
                res = True
    return(res)

# uses the tag_filter function to filter tweets
# filter(filterFunc: (T) ⇒ Boolean): DStream[T]
# Return a new DStream containing only the elements that satisfy a predicate
hashtags = dataStream.filter(tag_filter)

# finds the topic for the hashtag in the tweet by searching through
# the dicitionary values (#hashtags in the dictionary)
def topic_sort(line):
    res = ""
    for word in line.split(" "):
        for key in dic.keys():
            for value in dic[key]:
                if value == word.lower():
                    res = key
    return(res)

# uses nltk built-in Sentiment Intensity Analyzer (SIA) with the vader lexicon
# in order to rank the polarity of each tweet 
# compound values of 0.2 and -0.2 used
# reference: https://www.learndatasci.com/tutorials/sentiment-analysis-reddit-headlines-pythons-nltk/
def sentiment(tweet):
    sia = SIA()
    polarity = sia.polarity_scores(tweet)

    if polarity['compound'] > 0.2:
        return('pos')
    elif polarity['compound'] < -0.2:
        return('neg')
    else:
        return('neu')
    

# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (topic_sort(x) + "-" + sentiment(x), 1))

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

# checks if graph txt file exists and removes it before running
# used for graph.py
if os.path.exists('graph_data.txt'):
    os.remove('graph_data.txt')

# creates new files for graph data and output data
graph_data = open('graph_data.txt', 'a+')# keep apending to file
output = open('B_out.txt', 'a+')

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    output.write("----------- %s -----------\n" % str(time))
    try:
        all_rdd = rdd.take(1000)

        # print it nicely
        for tag in all_rdd:
            # write output to file
            graph_data.write('{:<40} {}\n'.format(tag[0], tag[1])) 
            output.write('{:<40} {}\n'.format(tag[0], tag[1]))
            print('{:<40} {}'.format(tag[0], tag[1]))
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

# do this for every single interval
hashtag_totals.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()

# close files
graph_data.close()
output.close()
