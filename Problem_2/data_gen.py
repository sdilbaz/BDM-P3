# -*- coding: utf-8 -*-
"""
Created on Wed Apr 15 09:06:44 2020

@author: Serdarcan Dilbaz
"""

import numpy as np
import os

def DataGen(number_of_points=float('inf')):
    np.random.seed(1)
    n=0
    while n<number_of_points:
        n+=1
        yield [str(val) for val in list(np.random.randint(1,10001,2))]
    
if __name__=='__main__':
    run_dir=os.path.dirname(os.path.abspath(__file__))
    
    with open(os.path.join(run_dir,"points_k_means.txt"),"w") as file:
        dgen = DataGen(number_of_points=8000000)
        for point in dgen:
            file.write('('+','.join(point)+')'+'\n')