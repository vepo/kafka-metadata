package io.vepo.kafka.metadata.producer;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerializer implements Serializer<Mensagem>, ClusterResourceListener {
    private static final Logger logger = LoggerFactory.getLogger(JsonSerializer.class);
    private ObjectMapper mapper;

    public JsonSerializer() {
        mapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(String topic, Mensagem data) {
        try {
            logger.info("Serializing: topic={} message={}", topic, data);
            return mapper.writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            return null;
        }
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        logger.info("Cluster: {}", clusterResource);

    }

}
