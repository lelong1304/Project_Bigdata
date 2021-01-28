from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("ConnectedComponents")
sc = SparkContext(conf = conf)


def ccfIterateMap(items) :
    list = []
    list.append([items[0],items[1]])
    list.append([items[1],items[0]])
    return list

def ccfIterateReduce(key, values) :
    list_couple=[]
    l=[]
    min = key
    for x in values:
        l.append(x)
        if (min>x):
            min = x
    if (min<key):
        list_couple.append((key,min))
        for x in l:
            if (min != x):
                list_couple.append((x,min))
    return list_couple

def main():
    list_input = sc.parallelize([(1,2),(2,3),(2,4),(4,5),(6,7),(7,8)])
    list_input = list_input.flatMap(lambda x : ccfIterateMap(x)).groupByKey().map(lambda x: (x[0],list(x[1]))).sortByKey(ascending=True)
    print(list_input.collect())
    list_input=list_input.flatMap(lambda x: ccfIterateReduce(x[0],list(x[1]))).sortByKey(ascending=True)
    print(list_input.collect())
main()
