package com.redislabs.sa.ot.util;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.*;
import redis.clients.jedis.providers.PooledConnectionProvider;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.time.Duration;


public class JedisConnectionHelper {
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
        return new Transaction(getPooledJedis().getPool().getResource());
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

    private static SSLSocketFactory createSslSocketFactory(
            String caCertPath, String caCertPassword, String userCertPath, String userCertPassword)
            throws IOException, GeneralSecurityException {

        KeyStore keyStore = KeyStore.getInstance("pkcs12");
        keyStore.load(new FileInputStream(userCertPath), userCertPassword.toCharArray());

        KeyStore trustStore = KeyStore.getInstance("jks");
        trustStore.load(new FileInputStream(caCertPath), caCertPassword.toCharArray());

        TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance("X509");
        trustManagerFactory.init(trustStore);

        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("PKIX");
        keyManagerFactory.init(keyStore, userCertPassword.toCharArray());

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagerFactory.getKeyManagers(), trustManagerFactory.getTrustManagers(), null);

        return sslContext.getSocketFactory();
    }

    public JedisConnectionHelper(JedisConnectionHelperSettings bs){
        System.out.println("Creating JedisConnectionHelper with "+bs);
        URI uri = buildURI(bs.getRedisHost(), bs.getRedisPort(), bs.getUserName(),bs.getPassword());
        HostAndPort address = new HostAndPort(uri.getHost(), uri.getPort());
        JedisClientConfig clientConfig = null;
        if(bs.isUsePassword()){
            String user = uri.getAuthority().split(":")[0];
            String password = uri.getAuthority().split(":")[1];
            password = password.split("@")[0];
            System.out.println("\n\nUsing user: "+user+" / password l!3*^rs@"+password);
            clientConfig = DefaultJedisClientConfig.builder().user(user).password(password)
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).timeoutMillis(bs.getRequestTimeoutMillis()).build(); // timeout and client settings

        }
        else {
            clientConfig = DefaultJedisClientConfig.builder()
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).timeoutMillis(bs.getRequestTimeoutMillis()).build(); // timeout and client settings
        }
        if(bs.isUseSSL()){ // manage client-side certificates to allow SSL handshake for connections
            SSLSocketFactory sslFactory = null;
            try{
                sslFactory = createSslSocketFactory(
                    bs.getCaCertPath(),
                    bs.getCaCertPassword(), // use the password you specified for keytool command
                    bs.getUserCertPath(),
                    bs.getUserCertPassword() // use the password you specified for openssl command
                );
            }catch(Throwable sslStuff){
                sslStuff.printStackTrace();
                System.exit(1);
            }
            clientConfig = DefaultJedisClientConfig.builder().user(bs.getUserName()).password(bs.getPassword())
                    .connectionTimeoutMillis(bs.getConnectionTimeoutMillis()).timeoutMillis(bs.getRequestTimeoutMillis())
            .sslSocketFactory(sslFactory) // key/trust details
                    .ssl(true).build();
        }
        GenericObjectPoolConfig<Connection> poolConfig = new ConnectionPoolConfig();
        poolConfig.setMaxIdle(bs.getPoolMaxIdle());
        poolConfig.setMaxTotal(bs.getMaxConnections());
        poolConfig.setMinIdle(bs.getPoolMinIdle());
        poolConfig.setMaxWait(Duration.ofMinutes(bs.getNumberOfMinutesForWaitDuration()));
        poolConfig.setTestOnCreate(bs.isTestOnCreate());
        poolConfig.setTestOnBorrow(bs.isTestOnBorrow());
        poolConfig.setNumTestsPerEvictionRun(bs.getNumTestsPerEvictionRun());
        poolConfig.setBlockWhenExhausted(bs.isBlockWhenExhausted());
        poolConfig.setMinEvictableIdleTime(Duration.ofMillis(bs.getMinEvictableIdleTimeMilliseconds()));
        poolConfig.setTimeBetweenEvictionRuns(Duration.ofMillis(bs.getTimeBetweenEvictionRunsMilliseconds()));

        this.connectionProvider = new PooledConnectionProvider(new ConnectionFactory(address, clientConfig), poolConfig);
        this.jedisPooled = new JedisPooled(connectionProvider);
        jedisPooled.set("com.redislabs.sa.ot.util.JedisConnectionHelper","test");
        jedisPooled.del("com.redislabs.sa.ot.util.JedisConnectionHelper");
    }
}

