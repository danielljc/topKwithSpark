import heapq
from pyspark import SparkContext, SparkConf
from operator import add

# sys.path.append('/app/spark/spark-2.3.1/python/lib')

if __name__ == '__main__':
    # conf = SparkConf().setAppName('TopK').setMaster('spark://master:7077')
    conf = SparkConf().setAppName('TopK').setMaster('spark://192.168.68.11:7077')
    sc = SparkContext(conf=conf)
    # 读取本地文件
    # variables = open('/home/spark/work/data/variables.txt', mode='r')
    variables = open('/home/homework02/variables.txt', mode='r')
    K = int(variables.readline().strip('\n'))
    dataPath = variables.readline().strip('\n')
    P = variables.readline().strip('\n')
    variables.close()

    # 读取HDFS文件
    # dataPathStr = "hdfs://master:9000" + dataPath
    dataPathStr = "hdfs://192.168.68.11:9000" + dataPath
    lines = sc.textFile(dataPathStr)
    # 执行操作
    pairRDD = lines\
        .flatMap(lambda line: line.split(" "))\
        .map(lambda kv: kv.split(","))\
        .mapValues(lambda v: float(v))\
        .reduceByKey(lambda x, y: round(x+y, 1))

    # 最小堆计算每个分区的topK,然后合并后再取topK
    parTopk = pairRDD.mapPartitions(lambda items: heapq.nlargest(K, items, key=lambda item: item[1]))
    res = heapq.nlargest(K, parTopk.collect(), key=lambda item: item[1])

    # 写入文件
    # filePath = '/home/spark/work/data/' + 'MF1832163.txt'
    filePath = P + 'MF1832163.txt'
    file = open(filePath, 'w')
    for i in range(K):
        file.write(res[i][0] + '\n')
    file.close()
