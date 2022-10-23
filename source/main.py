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

columns = ['GESTCEN', 'PXRACE', 'PRDTRACE', 'A_AGE', 'A_SEX', 'ERN_VAL', 'PHIP_VAL', 'PMED_VAL']

# https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra
for file in os.listdir('data'):
    f = os.path.join('data', file)
    if os.path.isfile(f) and 'sas7bdat' in f:
        df = pd.read_sas(f)
        # table = df.loc[:, columns]
        print(df)

# cluster.shutdown()

if __name__ == "__main__":
    print("Hello")
else:
    print("Bye")
