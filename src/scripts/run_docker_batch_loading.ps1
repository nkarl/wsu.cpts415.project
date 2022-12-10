docker compose up -d
python clean_data.py
echo "Loading test_data_cleaned.csv into Cassandra..."
docker cp "data/test_data_cleaned.csv" "wsu415_cassandra:/var/lib/test_data_prepped.csv"
docker cp "scripts/loaddb.cql" "wsu415_cassandra:/var/lib/loaddb.cql"
#docker exec -it wsu415_cassandra cqlsh
echo "Inserting data into Cassandra..."
