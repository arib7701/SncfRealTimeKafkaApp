package ribot.amandine.kafka.app;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import ribot.amandine.kafka.app.configuration.AppConfig;
import ribot.amandine.kafka.app.topology.DiscardDuplicatesTopology;

import java.util.Properties;

public class SncfDiscardDuplicates {

    private AppConfig appConfig;

    public static void main(String[] args) {

        SncfDiscardDuplicates disruptionsAggregation = new SncfDiscardDuplicates();
        disruptionsAggregation.start();
    }

    public SncfDiscardDuplicates() {

        appConfig = new AppConfig(ConfigFactory.load());
    }

    private Properties getKafkaStreamsConfig() {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

//        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
//        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl());

        return config;
    }

    private void start() {

        Properties properties = getKafkaStreamsConfig();
        new DiscardDuplicatesTopology(properties);
    }
}
