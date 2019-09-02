package ribot.amandine.kafka.app.runnable;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.KeyDisruption;
import ribot.amandine.kafka.app.Train;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

public class SncfAvroProducerThread implements Runnable {

    private final AppConfig appConfig;
    private final ArrayBlockingQueue<Disruption> disruptionsQueue;
    private final CountDownLatch latch;
    private final KafkaProducer<KeyDisruption, Disruption> kafkaProducer;
    private final String targetTopic;

    public SncfAvroProducerThread(AppConfig appConfig,
                                  ArrayBlockingQueue<Disruption> disruptionsQueue,
                                  CountDownLatch latch) {
        this.appConfig = appConfig;
        this.disruptionsQueue = disruptionsQueue;
        this.latch = latch;
        this.kafkaProducer = createKafkaProducer(appConfig);
        this.targetTopic = appConfig.getTopicName();
    }

    public KafkaProducer<KeyDisruption, Disruption> createKafkaProducer(AppConfig appConfig) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        properties.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        return new KafkaProducer<>(properties);
    }

    @Override
    public void run() {

        try {
            while (latch.getCount() > 1 || !disruptionsQueue.isEmpty()){
                Disruption disruption = disruptionsQueue.poll();
                if (disruption == null) {
                    Thread.sleep(200);
                } else {


                    if(disruption.getMessage() != null && !disruption.getStops().isEmpty())
                    {
                        final ProducerRecord<KeyDisruption, Disruption> record = new ProducerRecord<>(
                                targetTopic,
                                KeyDisruption.newBuilder().setId(disruption.getId()).build(),
                                disruption);

                        kafkaProducer.send(record, (metadata, exception) -> {
                            if (exception == null) {
                                //System.out.println(metadata);
                            } else {
                                exception.printStackTrace();
                            }
                        });
                    }

                    // sleeping to slow down the pace a bit
                    Thread.sleep(appConfig.getProducerFrequencyMs());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() {
        System.out.println("Closing Producer");
        kafkaProducer.flush();
        kafkaProducer.close();
        latch.countDown();
    }
}
