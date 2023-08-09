package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.RedisStreamWorkerGroupHelperV2;
import com.redislabs.sa.ot.streamutils.StreamEventMapProcessorV2;
import com.redislabs.sa.ot.util.JedisConnectionHelperSettings;
import redis.clients.jedis.JedisPooled;
import redis.clients.jedis.resps.StreamInfo;
import redis.clients.jedis.search.*;

import java.util.*;

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
    public static String JSON_KEY_PREFIX = "customer_order_history:";
    static boolean VERBOSE=false;
    static int PRINT_OUT_SKIP_SIZE =100;//used to limit the times messages are written to the screen
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
    static int ROUTING_VALUE_COUNT = 2; // by default, divide the streams into 2 groups / slots)
    static String INDEX_1_NAME = "idx_rouws1";
    static String INDEX_ALIAS_NAME = "idxa_rouws";
    static String OPERATIONS_STREAM_NAME = "X:OPERATIONS";
    static int howManyResultsToShow = 2;
    static int dialectVersion = 2;//3 provides full results from nested arrays

    public static void main(String [] args){
        ArrayList<String> argList = null;
        JedisConnectionHelperSettings settings = new JedisConnectionHelperSettings();

        if(args.length>0) {
            argList = new ArrayList<>(Arrays.asList(args));
            if (argList.contains("--host")) {
                int argIndex = argList.indexOf("--host");
                String host = argList.get(argIndex + 1);
                settings.setRedisHost(host);
            }
            if (argList.contains("--port")) {
                int argIndex = argList.indexOf("--port");
                int port = Integer.parseInt(argList.get(argIndex + 1));
                settings.setRedisPort(port);
            }
            if (argList.contains("--username")) {
                int argIndex = argList.indexOf("--username");
                String userName = argList.get(argIndex + 1);
                settings.setUserName(userName);
            }
            if (argList.contains("--password")) {
                int argIndex = argList.indexOf("--password");
                String password = argList.get(argIndex + 1);
                if(password!="") {
                    settings.setPassword(password);
                    settings.setUsePassword(true);
                }
            }
            if (argList.contains("--usessl")) {
                int argIndex = argList.indexOf("--usessl");
                boolean useSSL = Boolean.parseBoolean(argList.get(argIndex + 1));
                System.out.println("loading custom --usessl == " + useSSL);
                settings.setUseSSL(useSSL);
            }
            if (argList.contains("--cacertpath")) {
                int argIndex = argList.indexOf("--cacertpath");
                String caCertPath = argList.get(argIndex + 1);
                System.out.println("loading custom --cacertpath == " + caCertPath);
                settings.setCaCertPath(caCertPath);
            }
            if (argList.contains("--cacertpassword")) {
                int argIndex = argList.indexOf("--cacertpassword");
                String caCertPassword = argList.get(argIndex + 1);
                System.out.println("loading custom --cacertpassword == " + caCertPassword);
                settings.setCaCertPassword(caCertPassword);
            }
            if (argList.contains("--usercertpath")) {
                int argIndex = argList.indexOf("--usercertpath");
                String userCertPath = argList.get(argIndex + 1);
                System.out.println("loading custom --usercertpath == " + userCertPath);
                settings.setUserCertPath(userCertPath);
            }
            if (argList.contains("--usercertpass")) {
                int argIndex = argList.indexOf("--usercertpass");
                String userCertPassword = argList.get(argIndex + 1);
                System.out.println("loading custom --usercertpass == " + userCertPassword);
                settings.setUserCertPassword(userCertPassword);
            }

            if (argList.contains("--maxconnections")) {
                int argIndex = argList.indexOf("--maxconnections");
                MAX_CONNECTIONS = Integer.parseInt(argList.get(argIndex + 1));
                settings.setMaxConnections(MAX_CONNECTIONS);
            }
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
            if (argList.contains("--printoutskipsize")) {
                int argIndex = argList.indexOf("--printoutskipsize");
                PRINT_OUT_SKIP_SIZE = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--routingvaluecount")) {
                int argIndex = argList.indexOf("--routingvaluecount");
                ROUTING_VALUE_COUNT = Integer.parseInt(argList.get(argIndex + 1));
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
        settings.setTestOnBorrow(true);
        settings.setConnectionTimeoutMillis(120000);
        settings.setNumberOfMinutesForWaitDuration(1);
        settings.setNumTestsPerEvictionRun(10);
        settings.setPoolMaxIdle(1); //this means less stale connections
        settings.setPoolMinIdle(0);
        settings.setRequestTimeoutMillis(12000);
        settings.setTestOnReturn(false); // if idle, they will be mostly removed anyway
        settings.setTestOnCreate(true);

        com.redislabs.sa.ot.util.JedisConnectionHelper connectionHelper = null;
        try{
            connectionHelper = new com.redislabs.sa.ot.util.JedisConnectionHelper(settings); // only use a single connection based on the hostname (not ipaddress) if possible
        }catch(Throwable t){
            t.printStackTrace();
            try{
                Thread.sleep(4000);
            }catch(InterruptedException ie){}
            // give it another go - in case the first attempt was just unlucky:
            connectionHelper = new com.redislabs.sa.ot.util.JedisConnectionHelper(settings); // only use a single connection based on the hostname (not ipaddress) if possible
        }

//        JedisConnectionHelper connectionHelper = new JedisConnectionHelper(JedisConnectionHelper.buildURI(host,port,userName,password),MAX_CONNECTIONS);

        if(NUMBER_OF_WORKER_THREADS>0) { // we will have at least one consumer of streams:
            ArrayList<String> streamNamesFullList = new ArrayList<>();
            DummyOrderWriter tempDummyOrderWriter = new DummyOrderWriter();
            int loopValue = (HOW_MANY_ENTRIES / ROUTING_VALUE_COUNT);
            if(loopValue<ROUTING_VALUE_COUNT){loopValue=ROUTING_VALUE_COUNT;}
            for (int x = 0; x < loopValue; x++) { //many entries/order/Stream
                streamNamesFullList.add(tempDummyOrderWriter.getRouteEnrichedStreamName(ROUTING_VALUE_COUNT,STREAM_NAME_BASE, x));
            }
            int fullNameCount = streamNamesFullList.size();
            int divisor = (int) fullNameCount / ROUTING_VALUE_COUNT;
            if(divisor<1){divisor=1;}
            System.out.println("fullnameCount = " + fullNameCount + " divisor = " + divisor);
            for (int batch = 0; batch < ROUTING_VALUE_COUNT; batch++) {
                // we want to produce ROUTING_VALUE_COUNT-many streamNameLists and also that many RedisStreamWorkerGroupHelperV2's
                ArrayList<String> streamNamesList = new ArrayList<>();
                for (int y = 0; y < streamNamesFullList.size(); y++) {
                    if (y % ROUTING_VALUE_COUNT == batch) {
                        //collect the ones that are in this batch:
                        streamNamesList.add(streamNamesFullList.get(y));
                    }
                }

                RedisStreamWorkerGroupHelperV2 redisStreamWorkerGroupHelperV2 =
                        new RedisStreamWorkerGroupHelperV2()
                                .setPooledJedis(connectionHelper.getPooledJedis())
                                .setStreamNamesArrayList(streamNamesList)
                                .setVerbose(VERBOSE)
                                .setPrintoutSkipSize(PRINT_OUT_SKIP_SIZE);
                redisStreamWorkerGroupHelperV2.createConsumerGroup(PROCESSOR_GROUP_NAME);
                for (int w = 0; w < NUMBER_OF_WORKER_THREADS; w++) {
                    StreamEventMapProcessorV2 processor =
                            new StreamEventToJSONProcessorV2()
                                    .setJedisPooled(connectionHelper.getPooledJedis())
                                    .setSleepTime(WORKER_SLEEP_TIME)
                                    .setVerbose(VERBOSE)
                                    .setPrintoutSkipSize(PRINT_OUT_SKIP_SIZE);
                    String workerName = "worker" + (w + ADD_ON_DELTA_FOR_WORKER_NAME);
                    for (String streamName : streamNamesList) {
                        redisStreamWorkerGroupHelperV2.namedGroupConsumerStartListeningToAllStreams(workerName, processor);
                    }
                }
            }
        }
        DummyOrderWriter dummyOrderWriter = null;
        for(int wt=0;wt<NUMBER_OF_WRITER_THREADS;wt++){
            dummyOrderWriter = new DummyOrderWriter()
                    .setJedisPooled(connectionHelper.getPooledJedis())
                    .setSleepTime(WRITER_SLEEP_TIME)
                    .setRoutingValueCount(ROUTING_VALUE_COUNT)
                    .setTotalNumberToWrite(HOW_MANY_ENTRIES)
                    .setStreamNameBase(STREAM_NAME_BASE);
            dummyOrderWriter.kickOffStreamEvents();
        }
        //Create a search index
        createSearchIndex(connectionHelper.getPooledJedis());

        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis()<startTime+MAIN_LISTENER_DURATION) {//20 seconds of this:by default
            try {
                Thread.sleep(WORKER_SLEEP_TIME);
                String streamKeyName =  dummyOrderWriter.getRouteEnrichedStreamName(ROUTING_VALUE_COUNT,STREAM_NAME_BASE,(int)System.nanoTime() % ROUTING_VALUE_COUNT);
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
        //test the StreamUpdateJSONProcessor by
        //Registering a StreamUpdateJSONProcessor to the X:OPERATIONS stream
        //Using the index to search for a completed order with HAM
        //then creating a replace operation entry that will be worked on by our StreamUpdateJSONProcessor
        registerOperationsWorker(connectionHelper);

    }

    //TODO: figure out if this is worth implementing:
    static void getPathForJsonSearchResult(String query, SearchResult result){
        //JSON.ARRINDEX should allow us to find the paths based on the values returned
        /*
        JSON.ARRINDEX returns an array of integer replies for each path,
        the first position in the array of each JSON value that matches the path,
        -1 if unfound in the array, or nil, if the matching JSON value is not an array.
         */
    }

    //TODO: figure out if this is worth implementing:
    // (currently unused as queries are executed manually by User)
    static void searchForJSONObj(com.redislabs.sa.ot.util.JedisConnectionHelper connectionHelper){
        JedisPooled jedis = connectionHelper.getPooledJedis();
        String query = "@item1:()";
        SearchResult result = jedis.ftSearch(INDEX_ALIAS_NAME, new Query(query)
                .returnFields(
                        FieldName.of("event_name"),// This is a simple field from the root of the JSON doc (it is aliased in the index)
                        FieldName.of("$.location").as("EVENT_LOCATION"),// This is a simple field from the root of the JSON doc
                        FieldName.of("$.responsible_parties.hosts[?(@.name =~ \"(?i)^Chadw\")]") // note the ability to partially match with regex
                                .as("matched_party_by_name") // this demonstrates the discreet and aligned response capability
                ).limit(0,howManyResultsToShow).dialect(dialectVersion)
        );
        printResultsToScreen(query, result);
    }

    static void registerOperationsWorker(com.redislabs.sa.ot.util.JedisConnectionHelper connectionHelper){
        ArrayList<String> operationsStreamNameList = new ArrayList<String>();
        operationsStreamNameList.add(OPERATIONS_STREAM_NAME);
        RedisStreamWorkerGroupHelperV2 updateHelper =
                new RedisStreamWorkerGroupHelperV2()
                        .setPooledJedis(connectionHelper.getPooledJedis())
                        .setStreamNamesArrayList(operationsStreamNameList)
                        .setVerbose(VERBOSE)
                        .setPrintoutSkipSize(PRINT_OUT_SKIP_SIZE);
        updateHelper.createConsumerGroup(PROCESSOR_GROUP_NAME);
        //add processor
        StreamUpdateJSONProcessor worker =
                new StreamUpdateJSONProcessor()
                        .setJedisPooled(connectionHelper.getPooledJedis())
                        .setSleepTime(WORKER_SLEEP_TIME)
                        .setVerbose(VERBOSE)
                        .setPrintoutSkipSize(PRINT_OUT_SKIP_SIZE);
        String workerName = "updateWorker";
        updateHelper.namedGroupConsumerStartListeningToAllStreams(workerName, worker);
    }

    private static void printResultsToScreen(String query, SearchResult result){
        System.out.println("\n\tFired Query - \""+query+"\"\n Received a total of "+result.getTotalResults()+" results.\nDisplaying a maximum of "+howManyResultsToShow+" results:\n");

        List<Document> doclist = result.getDocuments();
        Iterator<Document> iterator = doclist.iterator();
        while (iterator.hasNext()) {
            Document d = iterator.next();
            Iterator<Map.Entry<String, Object>> propertiesIterator = d.getProperties().iterator();
            while (propertiesIterator.hasNext()) {
                Map.Entry<String, Object> pi = propertiesIterator.next();
                String propertyName = pi.getKey();
                Object propertyValue = pi.getValue();
                System.out.println(propertyName + " " + propertyValue);
            }
        }
    }

    /*
    FT.CREATE idx_rouws ON JSON PREFIX 1 customer_order_history:X
    SCHEMA
    $.RegionID AS region_id TAG SORTABLE
    $.order_stages[*].orderID AS order_id TAG
    $.order_stages[*].item1 AS item1 TAG
    $.order_stages[*].item2 AS item2 TAG
    $.order_stages[*].item3 AS item3 TAG
    $.order_stages[*].item4 AS item4 TAG
    $.order_stages[*].item5 AS item5 TAG
    $.order_stages[*].contact_name AS order_contact TEXT SORTABLE
    $.order_stages[*].order_cost AS order_cost NUMERIC SORTABLE
    $.order_stages[*].stage AS order_stage TAG

     */
    private static void createSearchIndex(JedisPooled jedis){

        Schema schema = new Schema().addField(new Schema.Field(FieldName.of("$.RegionID").as("region_id"),Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].stage").as("order_stage"),Schema.FieldType.TAG))
                .addSortableNumericField("$.order_stages[*].order_cost").as("order_cost")
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].orderID").as("order_id"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item1").as("item1"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item2").as("item2"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item3").as("item3"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item4").as("item4"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item5").as("item5"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item6").as("item6"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item7").as("item7"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item8").as("item8"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item9").as("item9"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item10").as("item10"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item11").as("item11"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item12").as("item12"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item13").as("item13"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item14").as("item14"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.order_stages[*].item15").as("item15"), Schema.FieldType.TAG))
                .addField(new Schema.Field(FieldName.of("$.location").as("location"), Schema.FieldType.TEXT))
                .addTextField("$.order_stages[*].contact_name", .75).as("order_contact"); //use with search 2.6.1 allows TEXT in multivalues
        IndexDefinition indexDefinition = new IndexDefinition(IndexDefinition.Type.JSON)
                .setPrefixes(new String[]{JSON_KEY_PREFIX});
        try {
            jedis.ftCreate(INDEX_1_NAME, IndexOptions.defaultOptions().setDefinition(indexDefinition), schema);
            //AND THEN: add schema alias so we can toggle between indexes:
            /*
            Added use of search index Alias (this allows for possible
            re-assigning of the alias to an alternate index that perhaps targets a different underlying dataset
            - maybe including additional or entirely different prefixes
            This example doesn't demonstrate that reassignment of the alias - just its effective use as a layer of indirection.
             */
            jedis.ftAliasAdd(INDEX_ALIAS_NAME, INDEX_1_NAME);
            System.out.println("Successfully created search index: " + INDEX_1_NAME + " and search index alias: " + INDEX_ALIAS_NAME);
        }catch(redis.clients.jedis.exceptions.JedisDataException jde){
            System.out.println("\n We can safely ignore this assuming we are happy with existing index: \n"+jde.getMessage());
        }
    }
}

