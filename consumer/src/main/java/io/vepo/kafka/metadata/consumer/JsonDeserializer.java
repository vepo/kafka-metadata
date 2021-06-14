package io.vepo.kafka.metadata.consumer;

import java.io.IOException;
import java.util.Arrays;

import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonDeserializer implements Deserializer<Mensagem>, ClusterResourceListener {
    private static final Logger logger = LoggerFactory.getLogger(JsonDeserializer.class);
    private ObjectMapper mapper;

    public JsonDeserializer() {
        mapper = new ObjectMapper();
    }

    @Override
    public Mensagem deserialize(String topic, byte[] data) {
        try {
            logger.info("Deserializing: topic={}, data={}", topic, Arrays.toString(data));
            return mapper.readValue(data, Mensagem.class);
        } catch (IOException e) {
            logger.error("Error!", e);
            return null;
        }
    }

    @Override
    public void onUpdate(ClusterResource clusterResource) {
        logger.info("Cluster: {}", clusterResource);
    }

}
