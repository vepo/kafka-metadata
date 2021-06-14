package io.vepo.kafka.metadata.producer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

public class SourceProducer {
    public static void main(String[] argv) throws Exception {
        int counter = 0;
        // Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        try (Producer<String, Mensagem> producer = new KafkaProducer<>(configProperties)) {
            do {
                ProducerRecord<String, Mensagem> rec = new ProducerRecord<>("first",
                        new Mensagem(counter++, "Mensagem: " + counter, System.currentTimeMillis()));
                rec.headers().add(new RecordHeader("id", UUID.randomUUID().toString().getBytes()));
                rec.headers().add(new RecordHeader("timestamp", long2Bytes(System.currentTimeMillis())));
                Future<RecordMetadata> results = producer.send(rec);
                RecordMetadata metadata = results.get();
                System.out.println(
                        "Message sent on partition=" + metadata.partition() + " with offset=" + metadata.offset());
                Thread.sleep(2000);
            } while (true);
        }
    }

    private static byte[] long2Bytes(long data) {
        return new byte[] { //
                (byte) ((data >> 56) & 0xff), //
                (byte) ((data >> 48) & 0xff), //
                (byte) ((data >> 40) & 0xff), //
                (byte) ((data >> 32) & 0xff), //
                (byte) ((data >> 24) & 0xff), //
                (byte) ((data >> 16) & 0xff), //
                (byte) ((data >> 8) & 0xff), //
                (byte) ((data >> 0) & 0xff) //
        };
    }
}