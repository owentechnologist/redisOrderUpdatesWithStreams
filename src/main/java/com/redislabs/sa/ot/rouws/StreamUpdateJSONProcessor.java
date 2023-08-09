package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessorV2;
import org.json.JSONObject;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.json.Path2;
import redis.clients.jedis.resps.StreamEntry;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class is responsible for updating values stored in existing JSON documents
 * The values are held in JSON objects that are part of an array
 * The class receives an entry, which contains a path to the element in the array that needs updating
 * using this path, this class either sets a new value, or deletes the old one
 * according to the operation key received in the the entry
 */
public class StreamUpdateJSONProcessor implements StreamEventMapProcessorV2 {
    static AtomicLong counter = new AtomicLong();
    private JedisPooled jedis = null;
    private Long sleepTime = null;//millis
    private boolean verbose = false;
    private String JSON_KEY_PREFIX="customer_order_history:";
    private int skipSize=1000;
    private long printcounter = 0;

    public StreamUpdateJSONProcessor setVerbose(boolean verbose){
        this.verbose=verbose;
        return this;
    }
    public StreamUpdateJSONProcessor setPrintoutSkipSize(int skipSize){
        this.skipSize = skipSize;
        return this;
    }

    public StreamUpdateJSONProcessor setJedisPooled(JedisPooled jedisPooled){
        this.jedis=jedisPooled;
        return this;
    }

    public StreamUpdateJSONProcessor setSleepTime(long sleepTime){
        this.sleepTime = sleepTime;
        return this;
    }

    @Override
    public void processStreamEntry(String streamName,StreamEntry payload) {
        printMessageSparingly("StreamUpdateJSONProcessor.processStreamEventMap..."+payload);
        String operation = streamName;
        String jsonKeyName = null;
        Path2 pathToUse = null;
        JSONObject orderStage = new JSONObject();
        ArrayList<JSONObject> orderStages = new ArrayList<>();

        printMessageSparingly(payload.toString());

        Map<String, String> map = payload.getFields();
        for( String f : map.keySet()){
            printMessageSparingly("key\t"+f+"\tvalue\t"+map.get(f));
            if(f.equalsIgnoreCase("order_cost")){
                orderStage.put(f,Float.parseFloat(map.get(f)));
            }else if (f.equalsIgnoreCase("jsonKeyName")){
                //we do not include the keyName in the data to be written
                //we will use it to identify the target document instead
                jsonKeyName=map.get(f);
            }else if (f.equalsIgnoreCase("pathToUse")){
                //we do not include the path in the data to be written
                //we will use it to identify the place in the document that needs a change
                pathToUse=Path2.of(map.get(f));
            }else{
                orderStage.put(f,map.get(f));
            }
        }
        // The orderStage JSON object holds the values from the map
        // maybe some food items and an order stage like 'new'
        orderStages.add(orderStage); // may only be a single map in a single event
        Path2 path = new Path2("$.order_stages");
        if(jedis.exists(jsonKeyName)){ // the JSON object can be updated
            if(operation.equalsIgnoreCase("replace")){
                jedis.jsonSet(jsonKeyName,pathToUse,orderStage);
            }else if(operation.equalsIgnoreCase("delete")){
                jedis.jsonDel(jsonKeyName,pathToUse);
            }
            //jedis.jsonArrAppend(JSON_KEY_PREFIX+streamName, path,orderStage);
        }else{ // create a new JSON Object (someone must have deleted the old one in between
            JSONObject obj = new JSONObject();
            String regionID = jsonKeyName.split("::")[1];//removing the "X:rouws::" prefix
            obj.put("RegionID",regionID);
            jedis.jsonSet(jsonKeyName,obj); // just the RegionID
            jedis.jsonSet(jsonKeyName,path,orderStages); //Now Add the OrderStages
        }
        printMessageSparingly("performed operation"+operation+" on this JSON Object in redis: "+jsonKeyName);
    }

    void printMessageSparingly(String message){
        if((printcounter%skipSize==0)&&(verbose)) {
            System.out.println("StreamUpdateJSONProcessor.message printed 1 time for each "+skipSize+" events:\n"+message);
        }
        printcounter++;
        if(printcounter==(skipSize+1)){printcounter=1;}//in case it's a long running service
    }
}
