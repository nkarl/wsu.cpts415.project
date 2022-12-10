0. Get data from Dylan's link:
```
https://1drv.ms/u/s!Ah-piEK7zfu0mUwY1tdwFMWDxtdG?e=npCknb
```

Copy data to `./src/data/test_data_prepped.csv`


1. At root folder, run:
```sh
cd src
docker compose up -d
docker cp ./cass_data_loading_test/test_data_prepped.csv wsu415_cassandra:/var/lib/test_data_prepped.csv
docker cp ./cass_data_loading_test/loaddb.cql wsu415_cassandra:/var/lib/loaddb.cql
docker exec -it wsu415_cassandra cqlsh
```

Then, run in `cqlsh`:
```sql
SOURCE '/var/lib/loaddb.cql';
```
