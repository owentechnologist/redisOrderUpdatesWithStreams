package com.redislabs.sa.ot.rouws;

import com.github.javafaker.Faker;
import redis.clients.jedis.JedisPooled;
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

    private JedisPooled jedis;
    public static final int STAGE_NEW=0,STAGE_ACCEPTED=1,STAGE_IN_PREPARATION=2,STAGE_OUT_FOR_DELIVERY=3,STAGE_COMPLETED=4,STAGE_CANCELLED=10;
    private String[] stages = {"new","accepted","in_preparation","out_for_delivery","completed"};
    private int routingValueCount=2;//default is all events go in 2 streams
    private long sleepTime = 50l;//milliseconds
    private long totalNumberToWrite = 1000;
    private String streamNameBase;
    private static Faker faker = new Faker();

    public DummyOrderWriter(){}//default constructor

    public DummyOrderWriter setRoutingValueCount(int routingValueCount){
        this.routingValueCount=routingValueCount;
        return this;
    }

    public DummyOrderWriter setStreamNameBase(String streamNameBase){
        this.streamNameBase=streamNameBase;
        System.out.println(this.getClass().getName()+"... setStreamBase() "+streamNameBase);
        return this;
    }

    public DummyOrderWriter setJedisPooled(JedisPooled jedis){
        this.jedis=jedis;
        return this;
    }

    public DummyOrderWriter setTotalNumberToWrite(long totalNumberToWrite){
        this.totalNumberToWrite=totalNumberToWrite;
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
                    //The number of streams is equal to routingValueCount
                    int streamID = (int) (System.nanoTime()%(totalNumberToWrite/routingValueCount));

                    map1 = buildCustomerOrderEvent(getRouteEnrichedHashKeyName(routingValueCount,streamNameBase,streamID));
                    jedis.xadd(getRouteEnrichedStreamName(routingValueCount,streamNameBase,streamID), XAddParams.xAddParams(), map1);
                    totalWrittenCounter++;
                    try{
                        Thread.sleep(sleepTime);
                    }catch(InterruptedException ie){}
                }
                System.out.println(this.getClass().getName()+": Wrote "+(totalWrittenCounter-1)+" messages.  Done.");
            }
        }).start();
    }

    //We need to allow for scenarios where multiple streams are read from in the same call
    //this demands that all of the streams share a routing value so they live in the same slot
    public String getRouteEnrichedStreamName(int routingValueCount,String streamNameBase,int id){
        String routingValue = "{"+(id%routingValueCount)+"}";
        return  getStreamName(streamNameBase,id)+routingValue;
    }

    public String getStreamName(String streamNameBase,int id){
        String pad = "0";
        for(int padx=100000000;padx>id;padx=padx/10){
            pad+="0";
        }
        return "X:"+streamNameBase+":"+pad+id;
    }
    //Same as for the Stream:
    public String getRouteEnrichedHashKeyName(int routingValueCount,String streamNameBase,int id){
        String routingValue = "{"+(id%routingValueCount)+"}";
        return  getHashKeyName(streamNameBase,id)+routingValue;
    }

    public String getHashKeyName(String streamNameBase,int id){
        return streamNameBase+":"+id+":order";
    }

    private HashMap<String,String> buildCustomerOrderEvent(String hashKeyName ) {
        String orderIDBase=hashKeyName.replaceAll("\\:","");
        orderIDBase=orderIDBase.replaceAll("\\{","");
        orderIDBase=orderIDBase.replaceAll("}","");
        HashMap<String,String> entryMap = new HashMap<>();
        int nextStage = STAGE_NEW;
        boolean newCustomer = true;
        long orderSeed=0;
        // determine a stage for event:
        if (jedis.exists(hashKeyName)) {
            newCustomer=false;
            //we use a Hash to keep track of the latest stage for that Order and the latest OrderID for a customer:
            nextStage = (int) jedis.hincrBy(hashKeyName,"stage",1);
            if(nextStage>=5){//cancelled or incremented beyond available options
                nextStage = STAGE_NEW;
            }
        }else{
            orderSeed = (long) jedis.hincrBy(hashKeyName,"orderSeed",1);
        }
        entryMap.put("stage",stages[nextStage]);
        if((System.nanoTime()%10==0) && nextStage<STAGE_COMPLETED && nextStage>STAGE_NEW ){
            //every so often an order gets delayed..
            entryMap.put("stage","delayed");
        }
        if(System.nanoTime()%120==0 && (!newCustomer) && nextStage>STAGE_NEW){
            //every so often an order gets cancelled..
            entryMap.put("stage","cancelled");
            nextStage=STAGE_CANCELLED;//using 10 as a flag to indicate a new orderID is needed
        }
        //create or update the string that tracks the stage for this order:
        jedis.hset(hashKeyName,"stage",""+nextStage);
        if(nextStage==STAGE_NEW){
            orderSeed = (long) jedis.hincrBy(hashKeyName,"orderSeed",1);
            // since this is a new order - we will add some food to it
            for(int x = 1;x<(System.nanoTime()%15)+1;x++) {
                entryMap.put("item"+x, faker.food().ingredient());
            }
            entryMap.put("contact_name",faker.name().fullName());
            entryMap.put("order_cost",3*System.nanoTime()%5+(entryMap.size()*7.99)+"");
            entryMap.put("orderID",orderIDBase+"__"+orderSeed);
        }else if(nextStage>STAGE_NEW){ //we are adding the orderID to the entry that captures any other order stage
            try {
                orderSeed = Long.parseLong(jedis.hget(hashKeyName, "orderSeed"));
                entryMap.put("orderID", orderIDBase + "__" + orderSeed);
            }catch(java.lang.NumberFormatException nfe){System.out.println("When faced with Number Format Exception... DummyOrderWriter built this orderID : "+orderIDBase + "__" + orderSeed); }//System.exit(0);}
        }
        return entryMap;
    }
}

