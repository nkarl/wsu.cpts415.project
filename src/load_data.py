import pandas as pd
from cassandra.cluster import Cluster

def load_into_cassandra(data):
    print("Loading into cassandra...\n")

    ## Creates Cassandra cluster, connects to default parameters, creates keyspace, and then sets the default keyspace via "USE".
    cluster = Cluster()
    session = cluster.connect("")

    session.execute("CREATE KEYSPACE IF NOT EXISTS cpts_415 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1}")
    session.execute("USE cpts_415")
    

    session.execute(f"""
        CREATE TABLE IF NOT EXISTS rate (
            id bigint PRIMARY KEY, 
            BusinessYear int, 
            StateCode varchar, 
            IssuerId int, 
            Age varchar, 
            IndividualRate float, 
            Race varchar, 
            Sex int, 
            AnnualIncome int, 
            AmountPaidMedical int
        )
    """)

    ## Inserts values into the table, iterating through each item in Dataframe data.
    query = "INSERT INTO rate (id, BusinessYear, StateCode, IssuerId, Age, IndividualRate, Race, Sex, AnnualIncome, AmountPaidMedical) VALUES (?,?,?,?,?,?,?,?,?,?)" 
    prepared = session.prepare(query)

    for i,j in data.iterrows():
        if (i % 1000 == 0):
            print(i)
        session.execute(prepared, (
            i + 1, 
            int(j["BusinessYear"]),
            str(j["StateCode"]),
            int(j["IssuerId"]),
            str(j["Age"]),
            j["IndividualRate"],
            str(j["Race"]),
            int(j["Sex"]),
            int(j["AnnualIncome"]),
            int(j["AmountPaidMedical"])
        ))

if __name__ == "__main__":
    newTestDataFrame = pd.read_csv("data/test_data.csv")
    load_into_cassandra(newTestDataFrame)