import pymongo
import time
import shutil
import _thread
import os
# 读取本地文件
variables = open('/home/spark/variables.txt', mode='r')
# variables = open('/home/spark/work/data/variables.txt', mode='r')

mongoPath = variables.readline().strip('\n')
DBName = variables.readline().strip('\n')
collectionName = variables.readline().strip('\n')
streamingPath = variables.readline().strip('\n')
P = variables.readline().strip('\n')
# print(mongoPath, DBName, collectionName, streamingPath, P)
variables.close()

client = pymongo.MongoClient('mongodb://' + mongoPath + '/')
dbDivorceCases = client[DBName]
col = dbDivorceCases[collectionName]

# 模拟流数据
def simulateData(threadName, delay):
    res = col.find({}, {"head": 1})
    count = col.count()
    for i in range(int(count / 50)):
        filepath = streamingPath + '/tmp/head' + str(time.time()) + '.log'
        logfile = open(filepath, 'w')
        for text in col.find({}, {"head": 1}).skip(i * 50).limit(50):
            logfile.write(text['head']['text'] + '\n')
            # print(text['head']['text'])
        logfile.close()
        print('---------------------' + str(i) + '------------------')
        shutil.move(filepath, streamingPath)
        time.sleep(delay)
    os._exit()


if __name__ == '__main__':
    # 开启模拟流数据线程
    try:
        _thread.start_new_thread(simulateData, ("simulateDataThread", 1))
    except:
        print("Error: 无法启动线程")

    while 1:
        pass




