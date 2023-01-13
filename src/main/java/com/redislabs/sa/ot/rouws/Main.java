package com.redislabs.sa.ot.rouws;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.PooledConnectionProvider;
import redis.clients.jedis.resps.StreamInfo;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
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
 *  An interested listener can register for realtime updates to a particular Stream
 *
 *  TODO: A worker converts the Stream entries to Hash objects in Redis
 *  TODO: These Hashes are indexed so that they can be searched ad-hoc
 */
public class Main {
    static boolean VERBOSE=false;
    static String STREAM_NAME_BASE = "rouws:";
    static String LISTENER_GROUP_NAME = "order_listeners";
    static String PROCESSOR_GROUP_NAME = "order_to_hash_processors";
    static int NUMBER_OF_WORKER_THREADS = 2;
    static int WRITER_BATCH_SIZE = 200;
    static long WRITER_SLEEP_TIME = 50l;//milliseconds
    static int HOW_MANY_ENTRIES = 100;
    static long WORKER_SLEEP_TIME = 50l;//milliseconds
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
            if (argList.contains("--mainlistenerduration")) {
                int argIndex = argList.indexOf("--mainlistenerduration");
                MAIN_LISTENER_DURATION = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--howmanyworkers")) {
                int argIndex = argList.indexOf("--howmanyworkers");
                NUMBER_OF_WORKER_THREADS = Integer.parseInt(argList.get(argIndex + 1));
            }
            if (argList.contains("--writerbatchsize")) {
                int argIndex = argList.indexOf("--writerbatchsize");
                WRITER_BATCH_SIZE = Integer.parseInt(argList.get(argIndex + 1));
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
        ConnectionHelper connectionHelper = new ConnectionHelper(ConnectionHelper.buildURI(host,port,userName,password));
        DummyOrderWriter dummyOrderWriter = new DummyOrderWriter(STREAM_NAME_BASE, connectionHelper.getPipeline())
                .setBatchSize(WRITER_BATCH_SIZE)
                .setJedisPooled(connectionHelper.getPooledJedis())
                .setSleepTime(WRITER_SLEEP_TIME)
                .setTotalNumberToWrite(HOW_MANY_ENTRIES);
        dummyOrderWriter.kickOffStreamEvents();
        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis()<startTime+MAIN_LISTENER_DURATION) {//20 seconds of this:by default
            try {
                Thread.sleep(WORKER_SLEEP_TIME*2);
                String streamKeyName =  dummyOrderWriter.getStreamName (STREAM_NAME_BASE,(int)System.nanoTime() % 20);
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

class ConnectionHelper{

    final PooledConnectionProvider connectionProvider;
    final JedisPooled jedisPooled;

    /**
     * Used when you want to send a batch of commands to the Redis Server
     * @return Pipeline
     */
    public Pipeline getPipeline(){
        return  new Pipeline(jedisPooled.getPool().getResource());
    }

    /**
     * Assuming use of Jedis 4.3.1:
     * https://github.com/redis/jedis/blob/82f286b4d1441cf15e32cc629c66b5c9caa0f286/src/main/java/redis/clients/jedis/Transaction.java#L22-L23
     * @return Transaction
     */
    public Transaction getTransaction(){
        return new Transaction(jedisPooled.getPool().getResource());
    }

    /**
     * Obtain the default object used to perform Redis commands
     * @return JedisPooled
     */
    public JedisPooled getPooledJedis(){
        return jedisPooled;
    }

    /**
     * Use this to build the URI expected in this classes' Constructor
     * @param host
     * @param port
     * @param username
     * @param password
     * @return
     */
    public static URI buildURI(String host, int port, String username, String password){
        URI uri = null;
        try {
            if (!("".equalsIgnoreCase(password))) {
                uri = new URI("redis://" + username + ":" + password + "@" + host + ":" + port);
            } else {
                uri = new URI("redis://" + host + ":" + port);
            }
        } catch (URISyntaxException use) {
            use.printStackTrace();
            System.exit(1);
        }
        return uri;
    }


    public ConnectionHelper(URI uri){
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        System.out.println("$$$ "+uri.getAuthority().split(":").length);
        if(uri.getAuthority().split(":").length==3){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            password = password.split("@")[0];
            System.out.println("\n\nUsing user: "+user+" / password @@@@@@@@@@"+password);
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings

        }else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(30000).timeoutMillis(120000).build(); // timeout and client settings
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(10);
        poolConfig.setMaxTotal(1000);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofMinutes(1));
        poolConfig.setTestOnCreate(true);

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
    }
}

