CREATE KEYSPACE IF NOT EXISTS cpts_415 WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};

USE cpts_415;

CREATE TABLE IF NOT EXISTS rate (
    id bigint PRIMARY KEY, 
    BusinessYear int, 
    StateCode text, 
    IssuerId int, 
    Age varchar, 
    IndividualRate float, 
    Race varchar, 
    Sex int, 
    AnnualIncome int, 
    AmountPaidMedical int
);

COPY cpts_415.rate FROM 'var/lib/test_data_prepped.csv' WITH DELIMITER=',' AND HEADER=TRUE;

