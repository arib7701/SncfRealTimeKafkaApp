package ribot.amandine.kafka.app.topology;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import ribot.amandine.kafka.app.Disruption;
import ribot.amandine.kafka.app.KeyDisruption;
import ribot.amandine.kafka.app.configuration.AppConfig;
import ribot.amandine.kafka.app.transformers.DiscardDuplicatesTransformer;

import java.util.Collections;
import java.util.Properties;

public class DiscardDuplicatesTopology {

    private final AppConfig appConfig;
    private final SpecificAvroSerde<Disruption> disruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private final SpecificAvroSerde<KeyDisruption> keyDisruptionSpecificAvroSerde = new SpecificAvroSerde<>();

    public DiscardDuplicatesTopology(Properties properties) {

        appConfig = new AppConfig(ConfigFactory.load());
        KafkaStreams streams = createTopology(properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private StoreBuilder<KeyValueStore<KeyDisruption, Disruption>> createStateStore(){

        return Stores
                .keyValueStoreBuilder((Stores.persistentKeyValueStore("disruptions-store")),
                        keyDisruptionSpecificAvroSerde,
                        disruptionSpecificAvroSerde);
    }

    private KafkaStreams createTopology(Properties properties) {

        keyDisruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), true);
        disruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(this.createStateStore());

        builder.stream(appConfig.getDisruptionTopicName(), Consumed.with(keyDisruptionSpecificAvroSerde, disruptionSpecificAvroSerde))
                .transform(DiscardDuplicatesTransformer::new, "disruptions-store")
                .peek(((key, value) -> System.out.println(value.toString())))
                .filter((key, value) -> key != null && value != null)
                .to(appConfig.getUniqueDisruptionTopicName());

        return new KafkaStreams(builder.build(), properties);
    }
}
