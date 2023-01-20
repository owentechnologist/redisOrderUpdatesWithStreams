package com.redislabs.sa.ot.streamutils;

import redis.clients.jedis.resps.StreamEntry;

public interface StreamEventMapProcessorV2 {
    public void processStreamEntry(String streamName, StreamEntry payload);
}
