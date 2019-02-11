from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer-order")
sc = SparkContext(conf = conf)
def parseLine(line):
    fields = line.split(',')
    id =   fields[0]
    cost = float(fields[2])
    return (id, cost)

lines = sc.textFile("c:///SparkCourse/customer-orders.csv")
rdd = lines.map(parseLine)
totalsById = rdd.reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0])).sortByKey()
totalsById = totalsById.collect()
for result in totalsById:
    print((int(result[1]), result[0]))