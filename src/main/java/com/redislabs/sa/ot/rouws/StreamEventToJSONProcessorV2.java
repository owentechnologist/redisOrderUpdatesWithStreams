package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessorV2;
import org.json.JSONObject;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StreamEventToJSONProcessorV2 implements StreamEventMapProcessorV2 {
    static AtomicLong counter = new AtomicLong();
    private JedisPooled jedis = null;
    private Long sleepTime = null;//millis
    private boolean verbose = false;
    private String JSON_KEY_PREFIX="customer_order_history:";
    private int skipSize=1000;
    private long printcounter = 0;

    public StreamEventToJSONProcessorV2 setJSONKeyPrefix(String jsonKeyPrefix){
        this.JSON_KEY_PREFIX=jsonKeyPrefix;
        return this;
    }

    public StreamEventToJSONProcessorV2 setVerbose(boolean verbose){
        this.verbose=verbose;
        return this;
    }
    public StreamEventToJSONProcessorV2 setPrintoutSkipSize(int skipSize){
        this.skipSize = skipSize;
        return this;
    }

    public StreamEventToJSONProcessorV2 setJedisPooled(JedisPooled jedisPooled){
        this.jedis=jedisPooled;
        return this;
    }

    public StreamEventToJSONProcessorV2 setSleepTime(long sleepTime){
        this.sleepTime = sleepTime;
        return this;
    }

    @Override
    public void processStreamEntry(String streamName,StreamEntry payload) {
        printMessageSparingly("StreamEventToJSONProcessor.processStreamEventMap..."+payload);

        JSONObject orderStage = new JSONObject();
        ArrayList<JSONObject> orderStages = new ArrayList<>();

        printMessageSparingly(payload.toString());
        Map<String, String> map = payload.getFields();
        for( String f : map.keySet()){
            printMessageSparingly("key\t"+f+"\tvalue\t"+map.get(f));
            if(f.equalsIgnoreCase("order_cost")){
                orderStage.put(f,Float.parseFloat(map.get(f)));
            }else{
                orderStage.put(f,map.get(f));
            }
        }
        // The orderStage JSON object holds the values from the map
        // maybe some food items and an order stage like 'new'
        orderStages.add(orderStage); // may only be a single map in a single event
        Path2 path = new Path2("$.order_stages");
        if(jedis.exists(JSON_KEY_PREFIX+streamName)){ // the JSON object can be appended
            jedis.jsonArrAppend(JSON_KEY_PREFIX+streamName, path,orderStage);
        }else{ // create a new JSON Object
            JSONObject obj = new JSONObject();
            String regionID = streamName.split("::")[1];//removing the "X:rouws::" prefix
            obj.put("RegionID",regionID);
            jedis.jsonSet(JSON_KEY_PREFIX+streamName,obj); // just the RegionID
            jedis.jsonSet(JSON_KEY_PREFIX+streamName,path,orderStages); //Now Add the OrderStages
        }
        printMessageSparingly("Changed this JSON Object in redis: "+ "json:"+streamName);
    }

    void printMessageSparingly(String message){
        if((printcounter%skipSize==0)&&(verbose)) {
            System.out.println("StreamEventToJSONProcessor.message printed 1 time for each "+skipSize+" events:\n"+message);
        }
        printcounter++;
        if(printcounter==(skipSize+1)){printcounter=1;}//in case it's a long running service
    }
}
