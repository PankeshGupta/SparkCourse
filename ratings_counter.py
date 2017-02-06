from pyspark import SparkContext, SparkConf
import os

# Create connection
conf = SparkConf().setMaster('local').setAppName('ratings_counter')
sc = SparkContext(conf=conf)

# generate RDD of strings
lines = sc.textFile(os.getcwd() + '/data/ml-100k/u.data')
# interested in only 2nd parameter
ratings = lines.map(lambda x: x.split()[2])
# countByValue returns dict where key is the
# value and value is the count of key
result = ratings.countByValue()
print result

for k in sorted(result.keys()):
    print k, result[k]
