from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from operator import add

# 读取本地文件
variables = open('/home/spark/variables.txt', mode='r')
# variables = open('/home/spark/work/data/variables.txt', mode='r')
mongoPath = variables.readline().strip('\n')
DBName = variables.readline().strip('\n')
collectionName = variables.readline().strip('\n')
streamingPath = variables.readline().strip('\n')
P = variables.readline().strip('\n')
variables.close()

list = ['北京市', '天津市', '上海市', '重庆市', '河北省', '山西省', '辽宁省', '吉林省', '黑龙江省', '江苏省', '浙江省', '安徽省', '福建省', '江西省', '山东省', '河南省', '湖北省', '湖南省', '广东省', '海南省', '四川省', '贵州省', '云南省', '陕西省', '甘肃省', '青海省', '台湾省', '内蒙古自治区', '广西壮族自治区', '西藏自治区', '宁夏回族自治区', '新疆维吾尔自治区']

# 过滤掉数据中不包含的省份，返回包含的省份名称
def filterProv(line):
    for prov in list:
        if line.find(prov) != -1:
            return prov


if __name__ == '__main__':
    conf = SparkConf()
    conf.setAppName('streamingTest')
    conf.setMaster('spark://192.168.68.11:7077')
    sc = SparkContext(conf=conf)

    outFilePath = P + 'MF1832163.txt'

    def writeToFile(rdd):
        provCountFile = open(outFilePath, 'a')
        countStr = ''
        coll = rdd.collect()
        if len(coll) != 0:
            for prov in coll:
                countStr += (str(prov[0]) + "_" + str(prov[1]) + ";")
            print(countStr)
            provCountFile.write(countStr + "\n")
            provCountFile.close()

    def updateSum(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    ssc = StreamingContext(sc, 10)
    # 监听文件夹
    files = ssc.textFileStream('file://' + streamingPath)
    ssc.checkpoint('file://' + streamingPath)

    # 对每个文件按行读取，过滤
    provs = files.flatMap(lambda lines: lines.split('\n')) \
        .map(lambda line: filterProv(str(line))) \
        .filter(lambda prov: prov is not None)

    # 计数
    provCount = provs.map(lambda x: (x, 1)).reduceByKey(add)
    provCount.foreachRDD(writeToFile)

    # 累加计数
    provCountSum = provCount.updateStateByKey(updateSum)
    provCountSum.foreachRDD(writeToFile)

    ssc.start()
    ssc.awaitTermination(timeout=1000)