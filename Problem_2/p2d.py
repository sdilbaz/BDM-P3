# -*- coding: utf-8 -*-
"""
Created on Fri Apr 17 05:02:52 2020

@author: Serdarcan Dilbaz
"""

from pyspark import SparkContext
import numpy as np
from operator import add
import itertools as it
import time

grid_size=500

def get_neighbors(tup):
    offsets=[np.array([-1,-1]),np.array([-1,0]),np.array([-1,1]),np.array([0,-1]),np.array([0,0]),np.array([0,1]),np.array([1,-1]),np.array([1,0]),np.array([1,1])]
    all_reg=np.array(tup)+np.stack(offsets)
    neigh=all_reg[np.logical_and.reduce((all_reg[:,0]>=0, all_reg[:,1]>=0, all_reg[:,0]<grid_size, all_reg[:,1]<grid_size))]
    return list(map(lambda a:tuple(a),list(neigh)))

def enrich(temp):
    return map(lambda a:(a,temp[1]),get_neighbors(temp[0]))

def to_list(a):
    return [a]

def append(a, b):
    a.append(b)
    return a

def extend(a, b):
    a.extend(b)
    return a

def tuple2id(tup):
    return tup[0]+tup[1]*grid_size+1

def neigh_group(tl):
    pop_n=[]
    def neigh_check(t1,t2):
        return abs(t1[0]-t2[0])<=1 and abs(t1[1]-t2[1])<=1
    for a,b in it.combinations(tl,2):
        if neigh_check(a,b):
            pop_n.append(["c-"+str(tuple2id(a)),"c-"+str(tuple2id(b))])
    return (len(pop_n),pop_n)

def neigh_group_aux(x):
    gg=neigh_group(x[1])
    return (x[0],gg[0],gg[1])

start_time=time.time()

sc =SparkContext()

raw_data = sc.textFile("points_k_means.txt")

data = raw_data.map(lambda x:np.array(x[1:-1].split(","),dtype='int'))
regionIds = data.map(lambda x: ((int((x[0]-1)/20),int((x[1]-1)/20)),1))

counts = regionIds.reduceByKey(add).map(lambda x:(x[1],x[0]))
groups = counts.combineByKey(to_list, append, extend)
pop_count = groups.map(lambda x:(x[0],len(x[1]),list(map(lambda d:"c-"+str(tuple2id(d)),x[1]))))
pop_neighbors = groups.map(neigh_group_aux,groups).filter(lambda x: x[1] != 0)

print("POP-COUNT SAMPLE: ",pop_count.take(2))
print("POP-COUNT SAMPLE: ",pop_neighbors.take(2))
print("Took %d seconds from start" %(time.time()-start_time))

##Took 84 seconds from start








