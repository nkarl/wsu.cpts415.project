"""
Source file for the project.
"""

import os
import pandas as pd
import re
import random
import numpy as np
from cassandra.cluster import Cluster

# import our parallel processing enngine
# import engine.core as parallel

# ip_address = '0.0.0.0'
# keyspace_name = ''
# cluster = Cluster(ip_address)
# session = cluster.connect()
# query = "CREATE DATABASE"   # change later
# session.execute(query)

columns = ['GESTCEN', 'PRDTRACE', 'A_AGE', 'A_SEX', 'ERN_VAL', 'PHIP_VAL', 'PMED_VAL']

# https://stackoverflow.com/questions/49108809/how-to-insert-pandas-dataframe-into-cassandra
print(os.listdir('data'))
sas_file = 'pppub18early_18par.sas7bdat'
f = os.path.join('data', sas_file)
pppubDF = pd.read_sas(f)

 ##create subset using necessarycolumns
table = pppubDF.loc[:, columns]

##convert gestcen data to str type ex(AK) = alaska
legend = open('data/censusstatecodelegend.txt').readlines()
states = {}
for line in legend:
    num, code = re.split(r'\t+', line)
    states[int(num)] = code.strip('\n')

##convert pracerecode data to str type ex(Black,White,Hispanic)
legend = open('data/pracerecodelegend.txt').readlines()
race = {}
for line in legend:
    code, description = re.split(r'\t+', line)
    description = description.strip(".\n").strip("only") # doesn't actually remove 'only' from all lines.
    print(code, description)
    race[int(code)] = description

i = 0
K_MARK = 1000
##Convert prdtrace to concatenate format
for row in range(0, len(table)):
    if i % K_MARK == 0:
        print(i)
    table.at[row, "PRDTRACE"] = race[int(table["PRDTRACE"][row])]
    ##Convert statecode to concatenate format
    table.at[row, "GESTCEN"] = states[int(table["GESTCEN"][row])]
    ##Convert Age to concatenate format
    table.at[row, "A_AGE"] = str(int(table.at[row, "A_AGE"]))
    ##Convert Insurance Rate to monthly for concatenation
    if table.at[row, "PHIP_VAL"] == 'Nan':
        table.at[row, "PHIP_VAL"] = 0
    else:
        table.at[row, "PHIP_VAL"] = str(round(int((table.at[row, "PHIP_VAL"])/12),2))
    i += 1
print(i)


table = table.rename({'GESTCEN': 'StateCode', 'A_AGE': 'Age', 'A_SEX': 'Sex', 'PHIP_VAL': 'IndividualRate', 'PRDTRACE' : 'Race', 'PMED_VAL': 'AmountPaidMedical', 'ERN_VAL': 'AnnualIncome'}, axis=1)

# making dataframe 
rateDF = pd.read_csv("data/archive/Rate.csv", usecols= ['BusinessYear', 'Age', 'StateCode', 'IssuerId', 'IndividualRate'])

joinedDF = pd.concat([rateDF, table])

# output the joined dataframe
#print(joinedDF)

# cluster.shutdown()

##Create larger dataset in order to experiment with Big Data query techniques by filling nulls, duplicating rows, and randomizing rows
def create_test_data(data):

    test = data

    print("Creating experimental dataset\n\nFilling Null Values ...\n\n")

    ##Fill NaN values with random selection from array with actual value
    for i,j in test.iterrows():
        for k in range(0,len(list(j))):
            if str(j[k]) == "nan" and str(j.keys()[k]) in table.columns:
                j[k] = (table[str(j.keys()[k])])[random.randint(0,len(table)-1)]
            elif str(j[k]) == "nan" and str(j.keys()[k]) in rateDF.columns:
                j[k] = (rateDF[str(j.keys()[k])])[random.randint(0, len(rateDF) - 1)]

    ##Duplicate randomized sets of test array to simulate new data
    pd.DataFrame.to_csv(test, 'data/import/test_data.csv', mode ='w')

    for s in range(1,200):
        newdata = test.sample(frac=1).reset_index()
        pd.DataFrame.to_csv(newdata, 'data/import/test_data.csv', mode ='a')
        print("Experimental dataset " + str(round((s/201),4) * 100) + "% complete.")

if __name__ == "__main__":
    pd.DataFrame.to_csv(joinedDF, "data/import/combined_data.csv")

    ## Create testing data set ### WARNING, execution time is slow, requires 130gb disk space.
    ## create_test_data(joinedDF)
