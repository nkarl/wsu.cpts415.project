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

/*COPY cpts_415.rate FROM 'var/lib/test_data_prepped.csv' WITH DELIMITER=',' AND HEADER=TRUE;*/
/*COPY cpts_415.rate (*/
    /*id bigint,*/
    /*BusinessYear int,*/
    /*StateCode varchar,*/
    /*IssuerId int,*/
    /*Age varchar,*/
    /*IndividualRate float,*/
    /*Race varchar,*/
    /*Sex int,*/
    /*AnnualIncome int,*/
    /*AmountPaidMedical int*/
    /*) FROM 'var/lib/test_data_prepped.csv';*/

COPY cpts_415.rate (
    id,
    BusinessYear,
    StateCode,
    IssuerId,
    Age,
    IndividualRate,
    Race,
    Sex,
    AnnualIncome,
    AmountPaidMedical
    ) FROM '/var/lib/test_data_cleaned.csv'
    WITH NULL='|' AND HEADER=TRUE;
