## This program is different from other Stream-based examples in that it uses potentially thousands or millions of Streams to allow for updates to thousands or millions of customerAccounts to be processed and consumed by interested Consumers 
### This program models a simple food delivery service
It shows how to create streams for each customer that will:
* 1) record their orders
* 2) notify any interested parties when the orders are at various stages:
*  - new
*  - accepted
*  - in_preparation
*  - out_for_delivery
*  - completed

## To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host <host> --port <port>"
```
## Note the program will not exit on its own as there are worker threads started and listening for Stream events

### * Using two or more separate shells, you can split the work up so that one instance runs the workers that consumer events, and the other writes new events

### 1. to run this program as an event consumer only with a worker capable of handling 4000 streams use:
``` 
 mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --howmanyworkers 1 --maxconnections 5000 --workersleeptime 20 --howmanywriters 0 --howmanyentries 20000"
```

### 2. To run this program as an event-publisher only with 50 writer threads for a couple of thousand writes / second and with a large target # of entries

```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --howmanyworkers 0 --howmanywriters 50 --howmanyentries 20000"
```
### Note you can also specify --username and --password

### Initial State (implemented==Done) (before adding JSON and Search)
![initialWorkflow](./initialWorkflow.png)
### Advanced State (implemented==in-progress) (after adding JSON and Search)
![advancedWorkflow](./advancedWorkflow.png)


### An interested listener can register for realtime updates to a particular Stream

The Main class loops through random streams, picking up the last message written to each one.

You can easily Browse the Streams and other keys in Redis using RedisInsight

Various other arguments can be passed to this program (see the main method for the possible --argname options)

### Streams are now being processed and JSON objects being created for each one
--SCALING THIS APPROACH IS BEING INVESTIGATED

* Problem statement:

Assuming no more than 10K connections at any one time - how to listen for changes on 10 million or more streams?
Also assume that updates are not more frequent on average than 1/second/stream 
There are likely to be long and frequent quiet periods

* thoughts: there can be worker groups with no active members...
1. Have helper listen to a single (or small number of) update stream(s) which informs it of the update(s) that actually happened
2. Have helper assign a worker to the actual active stream to process the latest (unprocessed) messages

### You can add a search index using the following command:
``` 
FT.CREATE idx_rouws ON JSON PREFIX 1 customer_order_history:X SCHEMA $.CustomerID AS CustomerID TAG $.order_stages[*].item1 AS item1 TAG $.order_stages[*].item2 AS item2 TAG $.order_stages[*].item3 AS item3 TAG $.order_stages[*].item4 AS item4 TAG $.order_stages[*].item5 AS item5 TAG $.order_stages[*].contact_name AS contact_name TEXT SORTABLE $.order_stages[*].order_cost AS order_cost NUMERIC SORTABLE $.order_stages[*].stage AS order_stage TAG SORTABLE
```

### You can search like this:

```
FT.SEARCH idx_rouws @order_stage:{cancelled} return 3 order_cost CustomerID order_stage limit 0 1
```
### And Aggregate like this:
``` 
FT.AGGREGATE idx_rouws "@item1:{Ham | Banana} @order_stage:{new}" GROUPBY 1 @order_cost REDUCE COUNT 0 AS number_matched DIALECT 1
```



