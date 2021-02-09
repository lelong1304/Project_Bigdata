
#Version1:

def ccfIterateReduce(key, values) :
    list_couple=[]
    counter=0
    min = key
    for x in values:
        if (min>x):
            min = x
    if (min<key):
        list_couple.append((key,min))
        for x in values:
            if (min != x):
                list_couple.append((x,min))
                counter+=1
    return (list_couple,counter)

def connected_components_v1(list_input):
    boole=False
    while(boole==False):
      list_input = list_input.flatMap(lambda x : [x,(x[1],x[0])]).groupByKey().flatMap(lambda x:[ccfIterateReduce(x[0],list(x[1]))])
      boole=list_input.filter(lambda x : x[1]!=0).isEmpty()
      list_input=list_input.flatMap(lambda x : x[0]).distinct()
    print(list_input.collect())

list_input = sc.parallelize([(1,2),(2,3),(2,4),(4,5),(6,7),(7,8)])
connected_components_v1(list_input)

#resultat : [(8, 6), (5, 1), (3, 1), (7, 6), (4, 1), (2, 1)]

#version 1.2:

def ccfIterateReduce(key, values) :
    list_couple=[]
    counter=0
    min = key
    for x in values:
        if (min>x):
            min = x
    if (min<key):
        list_couple.append((key,min))
        for x in values:
            if (min != x):
                list_couple.append((x,min))
                counter+=1
    return (list_couple,counter)

def connected_components_v1(list_input):
    boole=False
    while(boole==False):
      list_input = list_input.flatMap(lambda x : [x,(x[1],x[0])]).groupByKey().flatMap(lambda x:[ccfIterateReduce(x[0],list(x[1]))])
      boole=list_input.filter(lambda x : x[1]!=0).isEmpty()
      list_input=list_input.flatMap(lambda x : x[0]).distinct()
    print(list_input.sortBy(lambda x : x[1]).collect())

list_input = sc.parallelize([(1,2),(2,3),(2,4),(4,5),(6,7),(7,8)])
connected_components_v1(list_input)
#resultat : [(5, 1), (3, 1), (4, 1), (2, 1), (8, 6), (7, 6)]

#Version2

def ccfIterateReduce(key, values) :
    list_couple=[]
    counter=0
    min = values[0]
    if (min<key):
        list_couple.append((key,min))
        for x in values:
            if (min != x):
                list_couple.append((x,min))
                counter+=1
    return (list_couple,counter)

def connected_components_v2(list_input):
    boole=False
    while(boole==False):
      list_input = list_input.flatMap(lambda x : [x,(x[1],x[0])]).groupByKey().flatMap(lambda x:[ccfIterateReduce(x[0],sorted(x[1]))])
      boole=list_input.filter(lambda x : x[1]!=0).isEmpty()
      list_input=list_input.flatMap(lambda x : x[0]).distinct()
    print(list_input.collect())

list_input = sc.parallelize([(1,2),(2,3),(2,4),(4,5),(6,7),(7,8)])
connected_components_v2(list_input)

#resultat : [(8, 6), (5, 1), (3, 1), (7, 6), (4, 1), (2, 1)]

#version 2.2
def ccfIterateReduce_v2(key, values) :
    list_couple=[]
    counter=0
    min = values[0]
    if (min<key):
        list_couple.append((key,min))
        for x in values:
            if (min != x):
                list_couple.append((x,min))
                counter+=1
    return (list_couple,counter)

def connected_components_v3(list_input):
    boole=False
    while(boole==False):
      list_input = list_input.flatMap(lambda x : [x,(x[1],x[0])]).groupByKey().flatMap(lambda x:[ccfIterateReduce_v2(x[0],sorted(x[1]))])
      boole=list_input.filter(lambda x : x[1]!=0).isEmpty()
      list_input=list_input.flatMap(lambda x : x[0]).distinct()
    print(list_input.sortBy(lambda x : x[1]).collect())

list_input = sc.parallelize([(1,2),(2,3),(2,4),(4,5),(6,7),(7,8)])
connected_components_v3(list_input)

#resultat : [(5, 1), (3, 1), (4, 1), (2, 1), (8, 6), (7, 6)]



