package com.redislabs.sa.ot.rouws;

import com.github.javafaker.Faker;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.params.XAddParams;

import java.util.HashMap;
import java.util.Map;

/**
 * This class writes entries to many streams
 * Each stream represents a different customer and tracks details about that customer order history
 * An order is like a Hash object made of key-value pairs
 * An order has a keyname or unique id part of which maps to a customerID
 * this customerID is used to name each appearing stream
 * So a stream called X:rouws:12345 would hold order information for customerID 12345
 * Stages for the customer order are:
 *  *  - new
 *  *  - accepted
 *  *  - in_preparation
 *  *  - out_for_delivery
 *  *  - completed
 *  *  - cancelled
 *
 */
public class DummyOrderWriter {

    private Pipeline jedisPipeline;
    private JedisPooled jedis;
    private long sleepTime = 50l;//milliseconds
    private long batchSize = 200;
    private long totalNumberToWrite = 1000;
    private String streamNameBase;
    private static Faker faker = new Faker();
    private String[] stages = {"new","accepted","in_preparation","out_for_delivery","completed","cancelled"};

    public DummyOrderWriter(String streamNameBase, Pipeline jedisPipeline){
        this.jedisPipeline=jedisPipeline;
        this.streamNameBase=streamNameBase;
    }

    public DummyOrderWriter setJedisPooled(JedisPooled jedis){
        this.jedis=jedis;
        return this;
    }

    public DummyOrderWriter setTotalNumberToWrite(long totalNumberToWrite){
        this.totalNumberToWrite=totalNumberToWrite;
        return this;
    }

    public DummyOrderWriter setBatchSize(long batchSize){
        this.batchSize=batchSize;
        return this;
    }

    public DummyOrderWriter setSleepTime(long sleepTime){
        this.sleepTime=sleepTime;
        return this;
    }

    public void kickOffStreamEvents(){
        new Thread(new Runnable() {
            @Override
            public void run() {
                Map<String, String> map1 = new HashMap<>();
                long totalWrittenCounter = 1;
                while (totalWrittenCounter<=totalNumberToWrite) {
                    //generate a stream for the next order event:
                    int custID = (int) (System.nanoTime()%(totalNumberToWrite/5));
                    map1 = buildCustomerOrderEvent(getStringKeyName(streamNameBase,custID));
                    jedis.xadd(getStreamName(streamNameBase,custID), XAddParams.xAddParams(), map1);
                    totalWrittenCounter++;
                    try{
                        Thread.sleep(sleepTime);
                    }catch(InterruptedException ie){}
                }
            }
        }).start();
    }

    public String getStreamName(String streamNameBase,int id){
        return "X:"+streamNameBase+":"+id;
    }

    public String getStringKeyName(String streamNameBase,int id){
        return streamNameBase+":"+id+":stage";
    }

    private HashMap<String,String> buildCustomerOrderEvent(String stringKeyName ) {
        HashMap<String,String> entryMap = new HashMap<>();
        int nextStage = 0;
        // determine a stage for event:
        if (jedis.exists(stringKeyName)) {
            //we use a String to keep track of the latest stage for that Order:
            nextStage = (int) jedis.incr(stringKeyName);
            if(System.nanoTime()%222==0){
                //every so often an order gets cancelled..
                nextStage = 5;
            }
            if(nextStage<6){
                //write to stream some order information and the stage for the order
            }else{
                nextStage = 0;
                //change stage to new again (same customer is getting a new order going
            }
        }else{
            // we will create a String to represent the last known stage for the CustomerOrders
            nextStage = 0;
        }
        entryMap.put("stage",stages[nextStage]);
        //create or update the string that tracks the stage for this order:
        jedis.set(stringKeyName,""+nextStage);
        if(nextStage==0){
            // since this is a new order - we will add some food to it
            for(int x = 0;x<System.nanoTime()%10;x++) {
                entryMap.put("item"+x, faker.food().ingredient());
            }
            entryMap.put("contact_name",faker.name().fullName());
        }
        return entryMap;
    }

}

