package de.ing.simplestream.pigstream.kafka.json;

import java.util.Map;
import java.util.logging.LogManager;

import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonSerializer implements Serializer {

    

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Object o) {
        byte[] retVal = null;
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            retVal = objectMapper.writeValueAsBytes(o);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retVal;
    }

    @Override
    public void close() {

    }
}