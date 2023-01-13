## This program is different from other Stream-based examples in that it uses potentially thousands or millions of Streams to allow for updates to thousands or millions of customerAccounts to be processed and consumed by interested Consumers 
## To run the program with the default settings (supplying the host and port for Redis) do:
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host <host> --port <port>"
```
### To run this program with 10 writer threads for a couple of thousand writes / second and with a large target # of entries
```
mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host redis-10400.homelab.local --port 10400 --howmanyentries 2000000 --writersleeptime 0 --mainlistenerduration 120000 --howmanywriters 10"
```
### Note you can also specify --username and --password

### Initial State (implemented==Done) (before adding JSON and Search)
![initialWorkflow](./initialWorkflow.png)
### Advanced State (implemented==TODO) (after adding JSON and Search)
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


