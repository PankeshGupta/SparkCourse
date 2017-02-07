import os

from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local').setAppName('fake_friends')
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')
    age = int(fields[2])
    num_friends = int(fields[3])
    return age, num_friends


data_file = os.getcwd() + '/data/fakefriends.csv'
lines = sc.textFile(data_file)
rdd = lines.map(parse_line)
lis = rdd.mapValues(lambda x: (x, 1)) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda x: (x[0], x[1][0] / x[1][1])) \
    .collect()
# for x in sorted(lis, key=lambda x: x[1], reverse=True)[:5]:
#     print x
for z in sorted(rdd.mapValues(lambda x: (x, 1))
                        .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                        .map(lambda x: (x[0], x[1][0] / x[1][1]))
                        .collect(), key=lambda m: m[1], reverse=True)[:5]:
    print z
