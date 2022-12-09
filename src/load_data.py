import pandas as pd
from cassandra.cluster import Cluster

def cassandra_create_table(tableName, tableFields, session):
    query = "CREATE TABLE IF NOT EXISTS " + tableName + " (" + tableFields  + ");"
    session.execute(query)

def load_into_cassandra(data):
    print("Loading into cassandra...\n")

    ## Creates Cassandra cluster, connects to default parameters, creates keyspace, and then sets the default keyspace via "USE".
    cluster = Cluster()
    session = cluster.connect("")
    keyspace_name = "test"
    query = "CREATE KEYSPACE IF NOT EXISTS " + keyspace_name + " WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':1};"; 
    session.execute(query)
    session.execute("USE " + keyspace_name)
    
    ## Creates a table 
    tableName = "rate"
    tableFields = "BusinessYear int, StateCode text, IssuerId int, Age text, IndividualRate int, Race text, Sex int, AnnualIncome int, AmountPaidMedical int, Name int, PRIMARY KEY (Name)"
    cassandra_create_table(tableName, tableFields, session)

    ## Inserts values into the table, iterating through each item in Dataframe data.
    query = "INSERT INTO " + tableName + "(BusinessYear, StateCode, IssuerId, Age, IndividualRate, Race, Sex, AnnualIncome, AmountPaidMedical, Name) VALUES (?,?,?,?,?,?,?,?,?,?)" #Edit the values in the parentheses to insert different vals. Must append another "?" after VALUES for each additional field.
    prepared = session.prepare(query)

    ##Fill NaN values with random selection from array with actual value
    for i,j in data.iterrows():
        if (i % 100 == 0):
            print(i)
        session.execute(prepared, (int(j["BusinessYear"]),str(j["StateCode"]),int(j["IssuerId"]),str(j["Age"]),int(j["IndividualRate"]),str(j["Race"]),int(j["Sex"]),int(j["AnnualIncome"]),int(j["AmountPaidMedical"]),int(i+1)))

if __name__ == "__main__":
    newTestDataFrame = pd.read_csv("data/test_data.csv")
    load_into_cassandra(newTestDataFrame)