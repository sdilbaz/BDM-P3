# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 13:06:55 2020

@author: Serdarcan Dilbaz
"""

from pyspark import SparkContext
import numpy as np
from operator import add
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

start_time=time.time()
sc =SparkContext()

raw_data = sc.textFile("points_k_means.txt")

data = raw_data.map(lambda x:np.array(x[1:-1].split(","),dtype='int'))
regionIds = data.map(lambda x: ((int((x[0]-1)/20),int((x[1]-1)/20)),1))

counts = regionIds.reduceByKey(add)
enriched=counts.flatMap(enrich)
base=enriched.combineByKey(to_list, append, extend)

template = counts.join(base)
rd_scores=template.map(lambda x:(tuple2id(x[0]),x[1][0]/np.mean(x[1][1]))).sortBy(lambda x: x[1],ascending=False)
print("Top 16 grid cell:",rd_scores.take(16))
print("Took %d seconds from start" %(time.time()-start_time))

## Printout
#Top 16 grid cell: [(146074, 1.8214285714285714), (40908, 1.8), (205964, 1.7878787878787878), (151162, 1.786764705882353), (28089, 1.75), (34957, 1.7439446366782005), (46724, 1.7429577464788732), (221794, 1.7333333333333334), (48809, 1.7333333333333334), (23517, 1.7320754716981133), (52589, 1.7294117647058824), (128294, 1.7211155378486056), (48059, 1.7128027681660898), (61929, 1.7096774193548387), (60810, 1.7035714285714285), (38440, 1.6952054794520548)]
#Took 78 seconds from start