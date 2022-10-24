from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()
print(session.execute("SELECT release_version FROM system.local").one())