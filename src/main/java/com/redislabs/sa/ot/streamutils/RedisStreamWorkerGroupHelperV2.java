package com.redislabs.sa.ot.streamutils;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.*;

public class RedisStreamWorkerGroupHelperV2 {
    private JedisPooled jedisPooled = null;
    private ArrayList<String> streamNamesList = null;
    private String consumerGroupName;
    private int oneDay = 60 * 60 * 24 * 1000;
    private int fiveSeconds = 5000;
    private int oneMinute = 60000;
    private long printcounter = 0;
    private int skipSize = 1000;
    private boolean verbose = false;

    // Use this constructor for each consumer Group
    // In the case where you want multiple groups - create multiple instances of this class
    public RedisStreamWorkerGroupHelperV2() {
    }

    public RedisStreamWorkerGroupHelperV2 setStreamNamesArrayList(ArrayList<String> streamNamesList){
        this.streamNamesList = streamNamesList;
        System.out.println("Assigned this instance of RedisStreamWorkerGroupHelperV2 "+streamNamesList.size()+" streamNames...");
        return this;
    }
    public RedisStreamWorkerGroupHelperV2 setPooledJedis(JedisPooled pooledJedis){
        this.jedisPooled = pooledJedis;
        return this;
    }

    public RedisStreamWorkerGroupHelperV2 setVerbose(boolean verbose){
        this.verbose = verbose;
        return this;
    }

    public RedisStreamWorkerGroupHelperV2 setPrintoutSkipSize(int skipSize){
        this.skipSize = skipSize;
        return this;
    }


    // this classes' constructor determines the target StreamName(s)
    // we need to only provide the consumer group name
    public void createConsumerGroup(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
        StreamEntryID nextID = StreamEntryID.LAST_ENTRY; //This is the point at which the group begins
        for(String streamName:streamNamesList) {
            try {
                //all streams have the same groupname allowing multiple streams to be read in a single XREADGroup later on
                String thing = jedisPooled.xgroupCreate(streamName, this.consumerGroupName, nextID, true);
                printMessageSparingly(this.getClass().getName() + " : Result returned when creating a new ConsumerGroup " + thing);
                Thread.sleep(50l);
            } catch (Exception jde) {
                if (jde.getMessage().contains("BUSYGROUP")) {
                    printMessageSparingly("ConsumerGroup " + consumerGroupName + " already exists -- continuing");
                } else {
                    jde.printStackTrace();
                }
            }
        }
    }

    // This Method can be invoked multiple times each time with a unique consumerName
    // It assumes The group has been created - now we want a single named consumer to start
    // using 0 will grab any pending messages for that listener in case it failed mid-processing
    // a single consumer can handle multiple streams and requires fewer connections as a result <-- I hope
    public void namedGroupConsumerStartListeningToAllStreams(String consumerName, StreamEventMapProcessorV2 streamEventMapProcessorV2) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                //String key = "0"; // get all data for this consumer in case it is in recovery mode
                List<StreamEntry> streamEntryList = null;
                //StreamEntry value = null;
                StreamEntryID lastSeenID = null;
                printMessageSparingly("RedisStreamAdapter.namedGroupConsumerStartListeningToAllStreams(--> " + consumerName + "  <--): Actively Listening to "+streamNamesList.size()+" Streams");
                Map.Entry<String, StreamEntryID> streamQuery = null;
                /*
                with XREADGROUP you can read from multiple keys at the same time,
                however for this to work, you need to create a consumer group with the same name in every stream.
                ? Does this provide any benefit in terms of # of connections needed?
                 */
                while (true) { //loop eternally
                    try {
                        //grab up to 10 entries from the target streams at a time
                        //block for 5 seconds if no entries are immediately available in any stream
                        //this will return the number collected within those 5 seconds so that no messages are
                        //too delayed in being written due to waiting for other messages
                        //We will read from all Streams known to this instance of the helper:
                        //As the helper(s) is(are) created we can create sub-groups of streams
                        XReadGroupParams xReadGroupParams = new XReadGroupParams().block(fiveSeconds).count(10);
                        HashMap hashMap = new HashMap();
                        //here we add X # of streams to the Hashmap used in the XRead command:
                        //Note that we only add streamNames that have at least 1 entry
                        //Note that we also semi-randomly skip streams based on nanoTime
                        //This last decision is due to the fact that we try again in 5 seconds and want to limit
                        //the number of connections in use
                        for(int streamNamesCount=0;streamNamesCount<streamNamesList.size();streamNamesCount++) {
                            //if(jedisPooled.xlen(streamNamesList.get(streamNamesCount))>1){// && System.nanoTime()%3==0) {
                                hashMap.put(streamNamesList.get(streamNamesCount), StreamEntryID.UNRECEIVED_ENTRY);
                            //}
                        }
                        List<Map.Entry<String, List<StreamEntry>>> readGroupResult =
                                jedisPooled.xreadGroup(consumerGroupName, consumerName,
                                        xReadGroupParams,
                                        (Map<String, StreamEntryID>) hashMap);
                        //Results come from multiple Streams:

                        for(Map.Entry<String, List<StreamEntry>> readResult:readGroupResult) {
                            String streamName = readResult.getKey(); // name of Stream
                            streamEntryList = readResult.getValue(); // The entries captured from the stream
                            for(StreamEntry streamEntry:streamEntryList) {
                                printMessageSparingly("Consumer " + consumerName + " of ConsumerGroup " + consumerGroupName + " has received... " + streamName + " " + streamEntry);
                                lastSeenID = streamEntry.getID();
                                //only process one entry at a time:
                                streamEventMapProcessorV2.processStreamEntry(streamName, streamEntry);
                                jedisPooled.xack(streamName, consumerGroupName, lastSeenID);
                            }
                        }
                        //jedisPooled.xdel(key, lastSeenID);// Use trim in some other maintenance operation instead of delete here
                    }catch(NullPointerException npe){
                        printMessageSparingly("No new messages on the streams at this time");
                    }catch(redis.clients.jedis.exceptions.JedisConnectionException jce){
                        printMessageSparingly(" Unexpected end of stream.\n" +
                                "at com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelperV2$1.run ");
                        System.out.println(" Unexpected end of stream.\n" +
                                "at com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelperV2$1.run ");
                    }
                }
            }
        }).start();
    }


    void printMessageSparingly(String message){
        if((printcounter%skipSize==0)&&(verbose)) {
            System.out.println("This message printed 1 time for each "+skipSize+" events:\n"+message);
        }
        printcounter++;
        if(printcounter==(skipSize+1)){printcounter=1;}//in case it's a long running service
    }

}