package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;

import org.json.JSONObject;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StreamEventToJSONProcessor implements StreamEventMapProcessor {
    static AtomicLong counter = new AtomicLong();
    private JedisPooled jedis = null;
    private Long sleepTime = null;//millis
    private boolean verbose = false;
    private String JSON_KEY_PREFIX="customer_order_history:";

    public StreamEventToJSONProcessor setVerbose(boolean verbose){
        this.verbose=verbose;
        return this;
    }

    public StreamEventToJSONProcessor setJedisPooled(JedisPooled jedisPooled){
        this.jedis=jedisPooled;
        return this;
    }

    public StreamEventToJSONProcessor setSleepTime(long sleepTime){
        this.sleepTime = sleepTime;
        return this;
    }

    @Override
    public void processStreamEventMap(String streamName,Map<String, StreamEntry> payload) {
        //JedisPooled jedisPooled = jedisConnectionHelper.getPooledJedis();
        printMessageSparingly("StreamEventToJSONProcessor.processStreamEventMap..."+payload);

        JSONObject orderStage = new JSONObject();
        ArrayList<JSONObject> orderStages = new ArrayList<>();
        String originalId = "";

        for( String se : payload.keySet()) {
            StreamEntry x = payload.get(se); //x == the value which is a map
            printMessageSparingly(x.toString());

            Map<String, String> m = x.getFields();
            originalId = se.split(" ")[0]; //What ID was given to this map?
            m.put("OrderStageEventID",originalId);
            //orderStage = new JSONObject(m); <-- creates a problem if we want to index NUMERIC type
            for( String f : m.keySet()){
                printMessageSparingly("key\t"+f+"\tvalue\t"+m.get(f));
                if(f.equalsIgnoreCase("order_cost")){
                    orderStage.put(f,Float.parseFloat(m.get(f)));
                }else{
                    orderStage.put(f,m.get(f));
                }
            }

            // The orderStage JSON object holds the values from the map
            // maybe some food items and an order stage like 'new'
            orderStages.add(orderStage); // may only be a single map in a single event
        }
        Path2 path = new Path2("$.order_stages");
        if(jedis.exists(JSON_KEY_PREFIX+streamName)){ // the JSON object can be appended
            jedis.jsonArrAppend(JSON_KEY_PREFIX+streamName, path,orderStage);
        }else{ // create a new JSON Object
            JSONObject obj = new JSONObject();
            obj.put("CustomerID",streamName);// This id points to the Stream for that customer:
            jedis.jsonSet(JSON_KEY_PREFIX+streamName,obj); // just the CustomerID
            jedis.jsonSet(JSON_KEY_PREFIX+streamName,path,orderStages); //Now Add the OrderStages
        }
        printMessageSparingly("Changed this JSON Object in redis: "+ "json:"+streamName);
    }

    void printMessageSparingly(String message){
        int skipSize = 1000;
        if((counter.get()%skipSize==0)&&(verbose)) {
            System.out.println("StreamEventToJSONProcessor.message printed 1 time for each "+skipSize+" events:\n"+message);
        }
    }
}
