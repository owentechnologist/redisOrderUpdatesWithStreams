package com.redislabs.sa.ot.util;

public class JedisConnectionHelperSettings {
    private String redisHost = "";
    private int redisPort = 10000;
    private String userName = "default";
    private String password = "";
    private int maxConnections = 1000;
    private int connectionTimeoutMillis = 2000;
    private int requestTimeoutMillis = 2000;
    private int poolMaxIdle = 500;
    private int poolMinIdle = 50;
    private int numberOfMinutesForWaitDuration = 1;
    private boolean testOnCreate = true;
    private boolean testOnBorrow = true;
    private boolean testOnReturn = true;
    private int numTestsPerEvictionRun = 3;
    private boolean useSSL = false;
    private boolean usePassword = false;
    private long minEvictableIdleTimeMilliseconds = 30000;
    private long timeBetweenEvictionRunsMilliseconds = 1000;
    private boolean blockWhenExhausted = true;
    private String trustStoreFilePath = "";
    private String trustStoreType = "";
    private String caCertPath = "./truststore.jks";
    private String caCertPassword = "FIXME";
    private String userCertPath = "./redis-user-keystore.p12";
    private String userCertPassword = "FIXME";



    public String getRedisHost() {
        return redisHost;
    }

    public void setRedisHost(String redisHost) {
        this.redisHost = redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        this.setPoolMaxIdle(Math.round(maxConnections/2));
        this.setPoolMinIdle(Math.round(maxConnections/10));
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public void setConnectionTimeoutMillis(int connectionTimeoutMillis) {
        this.connectionTimeoutMillis = connectionTimeoutMillis;
    }

    public int getRequestTimeoutMillis() {
        return requestTimeoutMillis;
    }

    public void setRequestTimeoutMillis(int requestTimeoutMillis) {
        this.requestTimeoutMillis = requestTimeoutMillis;
    }

    public int getPoolMaxIdle() {
        return poolMaxIdle;
    }

    public void setPoolMaxIdle(int poolMaxIdle) {
        this.poolMaxIdle = poolMaxIdle;
    }

    public int getPoolMinIdle() {
        return poolMinIdle;
    }

    public void setPoolMinIdle(int poolMinIdle) {
        this.poolMinIdle = poolMinIdle;
    }

    public int getNumberOfMinutesForWaitDuration() {
        return numberOfMinutesForWaitDuration;
    }

    public void setNumberOfMinutesForWaitDuration(int numberOfMinutesForWaitDuration) {
        this.numberOfMinutesForWaitDuration = numberOfMinutesForWaitDuration;
    }

    public boolean isTestOnCreate() {
        return testOnCreate;
    }

    public void setTestOnCreate(boolean testOnCreate) {
        this.testOnCreate = testOnCreate;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public void setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public void setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
    }

    public int getNumTestsPerEvictionRun() {
        return numTestsPerEvictionRun;
    }

    public void setNumTestsPerEvictionRun(int numTestsPerEvictionRun) {
        this.numTestsPerEvictionRun = numTestsPerEvictionRun;
    }

    public boolean isUseSSL() {
        return useSSL;
    }

    public void setUseSSL(boolean useSSL) {
        this.useSSL = useSSL;
    }

    public boolean isUsePassword() {
        return usePassword;
    }

    public void setUsePassword(boolean usePassword) {
        this.usePassword = usePassword;
    }

    public long getMinEvictableIdleTimeMilliseconds() {
        return minEvictableIdleTimeMilliseconds;
    }

    public void setMinEvictableIdleTimeMilliseconds(long minEvictableIdleTimeMilliseconds) {
        this.minEvictableIdleTimeMilliseconds = minEvictableIdleTimeMilliseconds;
    }

    public long getTimeBetweenEvictionRunsMilliseconds() {
        return timeBetweenEvictionRunsMilliseconds;
    }

    public void setTimeBetweenEvictionRunsMilliseconds(long timeBetweenEvictionRunsMilliseconds) {
        this.timeBetweenEvictionRunsMilliseconds = timeBetweenEvictionRunsMilliseconds;
    }

    public boolean isBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public void setBlockWhenExhausted(boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
    }

    public String toString(){
        return "\nRedisUserName = "+getUserName()+"\nUsePassword = "+isUsePassword()+"\nUseSSL = "+isUseSSL()+ "\nRedisHost = "+getRedisHost()+
                "\nRedisPort = "+getRedisPort()+"\nMaxConnections = "+getMaxConnections()+
                "\nRequestTimeoutMilliseconds = "+getRequestTimeoutMillis()+"\nConnectionTimeOutMilliseconds = "+
                getConnectionTimeoutMillis();
    }

    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    public void setTrustStoreFilePath(String trustStoreFilePath) {
        this.trustStoreFilePath = trustStoreFilePath;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String trustStoreType) {
        this.trustStoreType = trustStoreType;
    }

    public String getCaCertPath() {
        return caCertPath;
    }

    public void setCaCertPath(String caCertPath) {
        this.caCertPath = caCertPath;
    }

    public String getCaCertPassword() {
        return caCertPassword;
    }

    public void setCaCertPassword(String caCertPassword) {
        this.caCertPassword = caCertPassword;
    }

    public String getUserCertPath() {
        return userCertPath;
    }

    public void setUserCertPath(String userCertPath) {
        this.userCertPath = userCertPath;
    }

    public String getUserCertPassword() {
        return userCertPassword;
    }

    public void setUserCertPassword(String userCertPassword) {
        this.userCertPassword = userCertPassword;
    }
}
