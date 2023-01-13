package com.redislabs.sa.ot.rouws;

import com.redislabs.sa.ot.streamutils.StreamEventMapProcessor;
import redis.clients.jedis.resps.StreamEntry;

import java.util.Map;

public class StreamEventToHashProcessor implements StreamEventMapProcessor {
    @Override
    public void processStreamEventMap(Map<String, StreamEntry> payload) {

    }
}
