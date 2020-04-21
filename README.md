# BDM-P3
Project 3 for CS585/DS503: Big Data Management
## Serdarcan Dilbaz and Hamidullah Sakhi
### Problem 1 SparkSQL for Processing Purchase Transactions (Hamidullah Sakhi)
#### Data Generation
Simple python code for generating the two datasets
#### SparkSQL
We used Pyspark in Jupyter notebook for the queries. 

### Problem 2 Spark-RDDs: Scala For Computing Relative-Density Scores at Scale (Serdarcan Dilbaz)
The logic for the Python and Scala implementation are identical. The pseudocode for problems 2.B, 2.C, 2.D will be outlined and the difference in execution times will be listed.

#### Problem 2.A: Create the Datasets
The data generation process is carried out in Python with a generator and uniform distribution for the points is assumed. To get a dataset of about 100MB, 8,000,000 coordinates were generated in the (x,y) format.

#### Problem 2.B: Report the TOP 10 grid cells with highest Relative-Density Scores

#### Problem 2.C: Report the TOP k grid cells w.r.t their Relative-Density Scores

#### Problem 2.D: Report groups of similarly populated cells and their connectedness

#### Python and Scala Time Comparison (in seconds)
| Problem | Scala | Python |
| ------------- | ------------- | ------------- |
| 2.B | 20 | 78 |
| 2.C | 37 | 185 |
| 2.D |  15 | 84 |
