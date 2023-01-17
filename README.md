## This program is different from other Stream-based examples in that it uses potentially thousands or millions of Streams to allow for updates to thousands or millions of customerAccounts to be processed and consumed by interested Consumers 
## To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host <host> --port <port>"
```
### To run this program with 10 writer threads for a couple of thousand writes / second and with a large target # of entries
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --howmanyentries 20000 --writersleeptime 0 --mainlistenerduration 120000 --howmanywriters 10"
```
### Note you can also specify --username and --password

### Initial State (implemented==Done) (before adding JSON and Search)
![initialWorkflow](./initialWorkflow.png)
### Advanced State (implemented==in-progress) (after adding JSON and Search)
![advancedWorkflow](./advancedWorkflow.png)

### This program models a simple food delivery service
It shows how to create streams for each customer that will:
* 1) record their orders
* 2) notify any interested parties when the orders are at various stages:
*  - new
*  - accepted
*  - in_preparation
*  - out_for_delivery
*  - completed

### An interested listener can register for realtime updates to a particular Stream

The Main class loops through random streams, picking up the last message written to each one.

You can easily Browse the Streams and other keys in Redis using RedisInsight

Various other arguments can be passed to this program (see the main method for the possible --argname options)

### Streams are now being processed and JSON objects being created for each one
--SCALING THIS APPROACH IS BEING INVESTIGATED
### You can add a search index using the following command:
``` 
FT.CREATE idx_rouws ON JSON PREFIX 1 orders:X SCHEMA $.CustomerID AS CustomerID TAG $.order_stages[*].item1 AS item1 TAG $.order_stages[*].item2 AS item2 TAG $.order_stages[*].item3 AS item3 TAG $.order_stages[*].item4 AS item4 TAG $.order_stages[*].item5 AS item5 TAG $.order_stages[*].contact_name AS contact_name TEXT SORTABLE $.order_stages[*].order_cost AS order_cost NUMERIC SORTABLE $.order_stages[*].stage AS order_stage TAG SORTABLE
```




