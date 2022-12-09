from cassandra.cluster import Cluster

ch = input("Running this script will DELETE all the data imported into cassandra\nEnter 'Y' to proceed: ")
if ch == 'y' or ch == 'Y':
    cluster = Cluster()
    session = cluster.connect("")
    session.execute("DROP KEYSPACE cpts_415")
