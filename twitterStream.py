from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import re
import numpy as np
import matplotlib.pyplot as plt
sc = None
pwords = None
nwords = None

def main():
    global sc, pwords, nwords
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")
   
    counts = stream(ssc, pwords, nwords, 100)
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    # YOUR CODE HERE
    positive, negative = zip(*counts)
    labelpos, poscount = zip(*positive)
    labelneg, negcount = zip(*negative)
    timestamps = range(0,len(counts))
    plt.plot(timestamps, poscount, marker = 'o', linestyle = '--', color = 'b', label = 'Positive Count')
    plt.plot(timestamps, negcount, marker = 'o', linestyle = '--', color = 'r', label = 'Negative Count')
    plt.xlabel("Time")
    plt.ylabel("Word Counts")
    plt.title("Twitter Sentiment Analyis")
    plt.legend()
    plt.show()


def load_wordlist(filename):
    """ 
    This function should return a list or set of words from the given filename.
    """
    # YOUR CODE HERE
    text = sc.textFile(filename)
    words = text.flatMap(lambda word: word.split("\n"))
    return words.collect()

def filterSpecChars(inp):
	#Following approach is inspired from a StackOverflow post
	return re.sub('[^A-Za-z0-9\s]', '', inp).lower()

def checkWord(word):
    if(word in pwords):
    	return ("positive", 1)
    elif(word in nwords):
    	return ("negative", 1)
    else:
    	return ("none", 1)

def updateFunction(newValues, runningCount):
    if runningCount is None:
       runningCount = 0
    return sum(newValues, runningCount)

def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    # YOUR CODE HERE

    
    tweets_filtered = tweets.map(filterSpecChars)

    tweets_words = tweets_filtered.flatMap(lambda word: word.split(" "))

    sentiment = tweets_words.map(checkWord)

    sentiment = sentiment.reduceByKey(lambda x, y : (int(x) + int(y)))

    sentiment = sentiment.filter(lambda x : x[0] != "none")

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    
    counts = []
    
    sentiment.foreachRDD(lambda t, rdd: counts.append(rdd.collect()))

    sentiment = sentiment.updateStateByKey(updateFunction)

    sentiment.pprint()
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
