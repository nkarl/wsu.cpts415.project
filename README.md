# WSU-2022Fall, CPTS415 PROJECT

## Getting started
This project requires Cassandra Docker image.

### Canssandra
First of all, make sure that your environment has Docker installed. 
Run
```sh
docker compose up -d
```

To directly use cqlsh interface in Cassandra container
```sh
docker exec -it wsu415_cassandra cqlsh
```

### Python packages
All the packages should be listed in ./requirements.txt
```sh
pip install -r requirements.txt
```

For Mac users:

```sh 
pip3 install -r requirements.txt
```
## Data
Please see [how to import data](https://github.com/nkarl/wsu.cpts415.project/tree/main/src/data)