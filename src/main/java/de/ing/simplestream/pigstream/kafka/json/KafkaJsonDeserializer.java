package de.ing.simplestream.pigstream.kafka.json;

import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class KafkaJsonDeserializer<T> implements Deserializer {

    

    private Class <T> type;
    
  

    public KafkaJsonDeserializer(Class type) {
        this.type = type;
    }

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Object deserialize(String s, byte[] bytes) {
        ObjectMapper mapper = new ObjectMapper();
        T obj = null;
        try {
            obj = mapper.readValue(bytes, type);
        } catch (Exception e) {

           e.printStackTrace();
        }
        return obj;
    }

    @Override
    public void close() {

    }
}