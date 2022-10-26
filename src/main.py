"""
Source file for the project.
"""

import os
import pandas as pd
import numpy as np
from cassandra.cluster import Cluster

# ip_address = '0.0.0.0'
# keyspace_name = ''
# cluster = Cluster(ip_address)
# session = cluster.connect()
# query = "CREATE DATABASE"   # change later
# session.execute(query)

columns = ['GESTCEN', 'PXRACE1', 'PRDTRACE', 'A_AGE', 'A_SEX', 'ERN_VAL', 'PHIP_VAL', 'PMED_VAL']

# https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra
print(os.listdir('data'))
sas_file = 'pppub18early_18par.sas7bdat'
f = os.path.join('data', sas_file)
pppubDF = pd.read_sas(f)
#print(pppubDF)
table = pppubDF.loc[:, columns]
#print(table)

# making dataframe 
rateDF = pd.read_csv("data/archive/Rate.csv") 

joinedDF = pppubDF.join(rateDF)

# output the joined dataframe
print(joinedDF)

# cluster.shutdown()

if __name__ == "__main__":
    print("Hello")
else:
    print("Bye")
