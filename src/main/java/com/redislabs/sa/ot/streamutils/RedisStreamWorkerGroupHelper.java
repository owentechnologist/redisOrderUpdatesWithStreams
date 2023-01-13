package com.redislabs.sa.ot.streamutils;

import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XReadGroupParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisStreamWorkerGroupHelper {
    private JedisPooled jedisPooled = null;

    private String streamName;
    private String consumerGroupName;
    private int oneDay = 60 * 60 * 24 * 1000;
    private long printcounter = 0;
    private boolean verbose = false;

    public RedisStreamWorkerGroupHelper(String streamName, JedisPooled jedisPooled, boolean verbose) {
        this.jedisPooled = jedisPooled;
        this.streamName = streamName;
        this.verbose = verbose;
    }

    // this classes' constructor determines the target StreamName
    // we need to only provide the consumer group name
    public void createConsumerGroup(String consumerGroupName) {
        this.consumerGroupName = consumerGroupName;
        StreamEntryID nextID = StreamEntryID.LAST_ENTRY; //This is the point at which the group begins
        try {
            String thing = jedisPooled.xgroupCreate(this.streamName, this.consumerGroupName, nextID, true);
            System.out.println(this.getClass().getName() + " : Result returned when creating a new ConsumerGroup " + thing);
        } catch (JedisDataException jde) {
            if (jde.getMessage().contains("BUSYGROUP")) {
                System.out.println("ConsumerGroup " + consumerGroupName + " already exists -- continuing");
            } else {
                jde.printStackTrace();
            }
        }
    }

    // This Method can be invoked multiple times each time with a unique consumerName
    // It assumes The group has been created - now we want a single named consumer to start
    // using 0 will grab any pending messages for that listener in case it failed mid-processing
    public void namedGroupConsumerStartListening(String consumerName, StreamEventMapProcessor streamEventMapProcessor) {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String key = "0"; // get all data for this consumer in case it is in recovery mode
                List<StreamEntry> streamEntryList = null;
                StreamEntry value = null;
                StreamEntryID lastSeenID = null;
                System.out.println("RedisStreamAdapter.namedGroupConsumerStartListening(--> " + consumerName + "  <--): Actively Listening to Stream " + streamName);
                long counter = 0;
                Map.Entry<String, StreamEntryID> streamQuery = null;

                while (true) {
                    //grab one entry from the target stream at a time
                    //block for long time if no entries are immediately available in the stream
                    XReadGroupParams xReadGroupParams = new XReadGroupParams().block(oneDay).count(1);
                    HashMap hashMap = new HashMap();
                    hashMap.put(streamName, StreamEntryID.UNRECEIVED_ENTRY);
                    List<Map.Entry<String, List<StreamEntry>>> streamResult =
                            jedisPooled.xreadGroup(consumerGroupName, consumerName,
                                    xReadGroupParams,
                                    (Map<String, StreamEntryID>) hashMap);
                    key = streamResult.get(0).getKey(); // name of Stream
                    streamEntryList = streamResult.get(0).getValue(); // we assume simple use of stream with a single update
                    value = streamEntryList.get(0);// entry written to stream
                    printMessageSparingly("Consumer " + consumerName + " of ConsumerGroup " + consumerGroupName + " has received... " + key + " " + value);

                    printcounter++;
                    Map<String, StreamEntry> entry = new HashMap();
                    entry.put(key + ":" + value.getID() + ":" + consumerName, value);
                    lastSeenID = value.getID();
                    streamEventMapProcessor.processStreamEventMap(entry);

                    jedisPooled.xack(key, consumerGroupName, lastSeenID);
                    jedisPooled.xdel(key, lastSeenID);// delete test
                }
            }
        }).start();
    }

    void printMessageSparingly(String message){
        int skipSize = 1000;
        if((printcounter%skipSize==0)&&(verbose)) {
            System.out.println("This message printed 1 time for each "+skipSize+" events:\n"+message);
        }
    }

}