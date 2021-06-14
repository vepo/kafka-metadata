package io.vepo.kafka.metadata.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FinalConsumer {
    private static final Logger logger = LoggerFactory.getLogger(FinalConsumer.class);

    public static void main(String[] argv) throws Exception {
        // Configure the Consumer
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "FinalConsumerGroup");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        try (Consumer<String, Mensagem> consumer = new KafkaConsumer<>(configProperties)) {
            consumer.subscribe(Arrays.asList("first"));
            while (true) {
                for (ConsumerRecord<String, Mensagem> message : consumer.poll(Duration.ofSeconds(1))) {
                    logger.info("Message received from partition={} with offset={}", message.partition(),
                            message.offset());
                    message.headers().forEach(header -> {
                        if (header.key().equals("id")) {
                            logger.info("Header: key={} value={}", header.key(), new String(header.value()));
                        } else {
                            logger.info("Header: key={} value={}", header.key(),
                                    convertByteArrayToLongArrayByShift(header.value()));
                        }
                    });
                    logger.info("Key={}\t value={}", message.key(), message.value());
                }
            }
        }
    }

    private static long convertByteArrayToLongArrayByShift(byte[] data) {
        return ((data[0] & 0xFFL) << 56) | //
                ((data[1] & 0xFFL) << 48) | //
                ((data[2] & 0xFFL) << 40) | //
                ((data[3] & 0xFFL) << 32) | //
                ((data[4] & 0xFFL) << 24) | //
                ((data[5] & 0xFFL) << 16) | //
                ((data[6] & 0xFFL) << 8) | //
                ((data[7] & 0xFFL) << 0);
    }
}
