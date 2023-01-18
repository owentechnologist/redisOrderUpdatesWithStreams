package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelper;
import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import redis.clients.jedis.resps.StreamInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

/**
 * * To run the program with the default settings (supplying the host and port for Redis) do:
 *  mvn compile exec:java -Dexec.cleanupDaemonThreads=false -Dexec.args="--host myhost.com --port 10000"
 *
 * This program models a simple food delivery service
 * It shows how to create streams for each customer that will:
 * 1) record their orders
 * 2) notify any interested parties when the orders are at various stages:
 *  - new
 *  - accepted
 *  - in_preparation
 *  - out_for_delivery
 *  - completed
 *  - cancelled
 *  An interested listener can register for realtime updates to a particular Stream
 *
 *  The RedisStreamWorkerGroupHelper registers the groups and workers to those groups
 *  Workers in workerGroups convert the Stream entries to JSON objects in Redis
 *  The worker type is the StreamEventToJSONProcessor

 *  TODO: These JSON objects are indexed so that they can be searched ad-hoc
 *  Execute the following command to create an index:
 FT.CREATE idx_rouws ON JSON PREFIX 1 customer_order_history:X SCHEMA $.CustomerID AS CustomerID TAG $.order_stages[*].item1 AS item1 TAG $.order_stages[*].item2 AS item2 TAG $.order_stages[*].item3 AS item3 TAG $.order_stages[*].item4 AS item4 TAG $.order_stages[*].item5 AS item5 TAG $.order_stages[*].contact_name AS contact_name TEXT SORTABLE $.order_stages[*].order_cost AS order_cost NUMERIC SORTABLE $.order_stages[*].stage AS order_stage TAG SORTABLE
 *  The JSON has multiple Orders stored within it for searching
 *  Searches will be possible at the order item level, contact name level and order state
 *  In this way, parent (customer) and child (orders) will be associated to one another
 *  For example: How many orders does customerX have?
 */
public class Main {

    static boolean VERBOSE=false;
    static int MAX_CONNECTIONS=1000;
    static String STREAM_NAME_BASE = "rouws:";
    static String LISTENER_GROUP_NAME = "order_listeners"; //TODO: scale the streamWorkerGroup logic
    static String PROCESSOR_GROUP_NAME = "order_to_json_processors"; //TODO: scale the streamWorkerGroup logic
    static int NUMBER_OF_WORKER_THREADS = 1; //TODO: scale the streamWorkerGroup logic
    static long WORKER_SLEEP_TIME = 50l;//milliseconds //TODO: scale the streamWorkerGroup logic
    public static int ADD_ON_DELTA_FOR_WORKER_NAME = 0;
    static long WRITER_SLEEP_TIME = 50l;//milliseconds
    static int NUMBER_OF_WRITER_THREADS = 1;
    static int HOW_MANY_ENTRIES = 100;
    static int MAIN_LISTENER_DURATION = 20000;//20 seconds

    public static void main(String [] args){
        ArrayList<String> argList = null;
        String host = "localhost";
        int port = 6379;
        String userName = "default";
        String password = "";

        if(args.length>0) {
            argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--verbose")) {
                int argIndex = argList.indexOf("--verbose");
                VERBOSE = Boolean.parseBoolean(argList.get(argIndex + 1));
            }
            if (argList.contains("--streamnamebase")) {
                int argIndex = argList.indexOf("--streamnamebase");
                STREAM_NAME_BASE = argList.get(argIndex + 1);
            }
            if (argList.contains("--processorgroupname")) {
                int argIndex = argList.indexOf("--processorgroupname");
                PROCESSOR_GROUP_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--listenergroupname")) {
                int argIndex = argList.indexOf("--listenergroupname");
                LISTENER_GROUP_NAME = argList.get(argIndex + 1);
            }
            if (argList.contains("--host")) {
                int argIndex = argList.indexOf("--host");
                host = argList.get(argIndex + 1);
            }
            if (argList.contains("--port")) {
                int argIndex = argList.indexOf("--port");
                port = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--username")) {
                int argIndex = argList.indexOf("--username");
                userName = argList.get(argIndex + 1);
            }
            if (argList.contains("--password")) {
                int argIndex = argList.indexOf("--password");
                password = argList.get(argIndex + 1);
            }
            if (argList.contains("--maxconnections")) {
                int argIndex = argList.indexOf("--maxconnections");
                MAX_CONNECTIONS = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--mainlistenerduration")) {
                int argIndex = argList.indexOf("--mainlistenerduration");
                MAIN_LISTENER_DURATION = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyworkers")) {
                int argIndex = argList.indexOf("--howmanyworkers");
                NUMBER_OF_WORKER_THREADS = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--addondeltaforworkername")) {
                int argIndex = argList.indexOf("--addondeltaforworkername");
                ADD_ON_DELTA_FOR_WORKER_NAME = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanywriters")) {
                int argIndex = argList.indexOf("--howmanywriters");
                NUMBER_OF_WRITER_THREADS = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--writersleeptime")) {
                int argIndex = argList.indexOf("--writersleeptime");
                WRITER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyentries")) {
                int argIndex = argList.indexOf("--howmanyentries");
                HOW_MANY_ENTRIES = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--workersleeptime")) {
                int argIndex = argList.indexOf("--workersleeptime");
                WORKER_SLEEP_TIME = Integer.parseInt(argList.get(argIndex + 1));
            }
        }

        JedisConnectionHelper connectionHelper = new JedisConnectionHelper(JedisConnectionHelper.buildURI(host,port,userName,password),MAX_CONNECTIONS);

        if(NUMBER_OF_WORKER_THREADS>0){
            ArrayList<String> streamNamesList = new ArrayList<>();
            DummyOrderWriter tempDummyOrderWriter = new DummyOrderWriter();
            for(int x=0;x<(HOW_MANY_ENTRIES/5);x++) {
                streamNamesList.add(tempDummyOrderWriter.getStreamName(STREAM_NAME_BASE, x));
            }
            RedisStreamWorkerGroupHelper redisStreamWorkerGroupHelper =
                new RedisStreamWorkerGroupHelper(streamNamesList, connectionHelper.getPooledJedis(), VERBOSE);
            redisStreamWorkerGroupHelper.createConsumerGroup(PROCESSOR_GROUP_NAME);
            for(int w=0;w<NUMBER_OF_WORKER_THREADS;w++){
                StreamEventMapProcessor processor =
                        new StreamEventToJSONProcessor()
                                .setJedisPooled(connectionHelper.getPooledJedis())
                                .setSleepTime(WORKER_SLEEP_TIME)
                                .setVerbose(VERBOSE);
                String workerName = "worker"+(w+ADD_ON_DELTA_FOR_WORKER_NAME);
                for(String streamName:streamNamesList) {
                    redisStreamWorkerGroupHelper.namedGroupConsumerStartListening(streamName,workerName, processor);
                }
            }
        }
        DummyOrderWriter dummyOrderWriter = null;
        for(int wt=0;wt<NUMBER_OF_WRITER_THREADS;wt++){
            dummyOrderWriter = new DummyOrderWriter(STREAM_NAME_BASE, connectionHelper.getPipeline())
                    .setJedisPooled(connectionHelper.getPooledJedis())
                    .setSleepTime(WRITER_SLEEP_TIME)
                    .setTotalNumberToWrite(HOW_MANY_ENTRIES);
            dummyOrderWriter.kickOffStreamEvents();
        }

        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis()<startTime+MAIN_LISTENER_DURATION) {//20 seconds of this:by default
            try {
                Thread.sleep(WORKER_SLEEP_TIME);
                String streamKeyName =  dummyOrderWriter.getStreamName (STREAM_NAME_BASE,(int)System.nanoTime() % HOW_MANY_ENTRIES);
                StreamInfo message = connectionHelper.getPooledJedis().xinfoStream(streamKeyName);
                Map<String, String> entryFields = message.getLastEntry().getFields();
                Set<String> keySet = entryFields.keySet();
                System.out.println("\t Got message/entry from Stream with key name of: "+streamKeyName);
                for (String key : keySet) {
                    System.out.println(key + ": " + entryFields.get(key));
                }
            } catch (Throwable t) {
            }
        }
    }
}

