package ribot.amandine.kafka.app.runnable;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class SncfAvroProducerThread implements Runnable {

    private Logger log = LoggerFactory.getLogger(SncfAvroProducerThread.class.getSimpleName());

    private final AppConfig appConfig;
    private final ArrayBlockingQueue<Train> trainsQueue;
    private final CountDownLatch latch;
    private final KafkaProducer<Long, Train> kafkaProducer;
    private final String targetTopic;

    public SncfAvroProducerThread(AppConfig appConfig,
                                  ArrayBlockingQueue<Train> trainsQueue,
                                  CountDownLatch latch) {
        this.appConfig = appConfig;
        this.trainsQueue = trainsQueue;
        this.latch = latch;
        this.kafkaProducer = createKafkaProducer(appConfig);
        this.targetTopic = appConfig.getTopicName();
    }

    public KafkaProducer<Long, Train> createKafkaProducer(AppConfig appConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {
        int trainCount = 0;
        try {
            while (latch.getCount() > 1 || trainsQueue.size() > 0){
                Train train = trainsQueue.poll();
                if (train == null) {
                    Thread.sleep(200);
                } else {
                    trainCount += 1;
                    log.info("Sending train " + trainCount + ": " + train);
                    kafkaProducer.send(new ProducerRecord<>(targetTopic, train));
                    // sleeping to slow down the pace a bit
                    Thread.sleep(appConfig.getProducerFrequencyMs());
                }
            }
        } catch (InterruptedException e) {
            log.warn("Avro Producer interrupted");
        } finally {
            close();
        }
    }

    public void close() {
        log.info("Closing Producer");
        kafkaProducer.close();
        latch.countDown();
    }
}
