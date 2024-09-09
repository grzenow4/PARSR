# Authors
This project was written by Wiktor Grzankowski and Grzegorz Nowakowski as final task for Practical Distributed Systems at MIMUW faculty.

# App setup
Node vm101 is responsible for load balancing using haproxy and by default for starting the app.

Nodes vm[02:04] are kafka brokers for input topic, which has 10 partitions, replication factor = 2 and retention time = 5 min.

Nodes vm[02:06] are aerospike database nodes, with namespace parsr having replication factor = 2.

Nodes vm[07:10] are java applications. Each instance is stateless and new nodes can be added according to demand.

# How to run the app
Clone the repository on vm101. Everywhere, switch st124 to st\<your number\>.

Execute the following commands:
```
./deploy.sh <user> <password>
```
This command deploys the app across all nodes, even on completely newly started machines.

For first deploy, you will be asked once to press Y to agree to upgrade.

Important: before each start of the testing platform, cleanup the VMs.
```
./clean.sh <user> <password>
```
This script stops all docker containers, stops aerospike databases and kafka brokers, cleans up the database and kafka topics.

# Additional information
1. Haproxy load balancer uses /health endpoint for frequent healthcheck of java nodes, allowing it for quick updates in case of a node outage.
1. UC3 is implemented using KafkaStreams, allowing it to naturally scale horizontally. During testing, we noticed that only around 80% of /aggregate requests succeed, suspecting that it is a result of imperfect configuration of aggregation strategy of the KTable. After aggregation, results are written to Aerospike with retention time = 24h.
1. UC1 and UC2 work close to 100% success rate, with some rare drops occuring only on some runs, mostly in the earliest time when starting the application.
