import pandas as pd
from cassandra.cluster import Cluster


def perform_type_conversion(data):
    """
    Clean the CSV before loading into Cassandra.
    """
    print("Converting column types...\n")
    test = pd.read_csv("data/test_data.csv")
    test['Sex'] = test['Sex'].astype(int)
    test['IssuerId'] = test['IssuerId'].astype(int)
    test['BusinessYear'] = test['BusinessYear'].astype(int)
    test['AnnualIncome'] = test['AnnualIncome'].astype(int)
    test['AmountPaidMedical'] = test['AmountPaidMedical'].astype(int)
    test.drop(index=test.index[0], axis=0, inplace=True)
    test = test.drop(test.columns[[0]], axis=1)

    pd.DataFrame.to_csv(test, 'data/test_data_cleaned.csv', mode ='w')    # test = pd.read_csv("data/test_data.csv")
    pass


if __name__ == "__main__":
    newTestDataFrame = pd.read_csv("data/test_data.csv")
    perform_type_conversion(newTestDataFrame)
    print("Cleaning data is complete.")
