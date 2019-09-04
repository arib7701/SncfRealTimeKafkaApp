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
import ribot.amandine.kafka.app.aggregators.ComplexAggregators;
import ribot.amandine.kafka.app.aggregators.SimpleAggregators;
import ribot.amandine.kafka.app.configuration.AppConfig;
import ribot.amandine.kafka.app.transformers.DisruptionTransformer;

import java.util.Collections;
import java.util.Properties;

public class DisruptionsAggregatorTopology {

    private final AppConfig appConfig;
    private final SpecificAvroSerde<Disruption> disruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private final SpecificAvroSerde<KeyDisruption> keyDisruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private SimpleAggregators simpleAggregators;
    private ComplexAggregators complexAggregators;

    public DisruptionsAggregatorTopology(Properties properties) {

        appConfig = new AppConfig(ConfigFactory.load());
        simpleAggregators = new SimpleAggregators();
        complexAggregators = new ComplexAggregators();
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

        KStream<KeyDisruption, Disruption> disruptionKStream = builder
                .stream(appConfig.getDisruptionTopicName(), Consumed.with(keyDisruptionSpecificAvroSerde, disruptionSpecificAvroSerde))
                .transform(DisruptionTransformer::new, "disruptions-store")
                .peek(((key, value) -> System.out.println(value.toString())))
                .filter((key, value) -> key != null && value != null);

        simpleAggregators.aggregateStopFromStream(disruptionKStream);
        simpleAggregators.aggregateTrainFromStream(disruptionKStream);
        simpleAggregators.aggregateCauseFromStream(disruptionKStream);

        complexAggregators.aggregateByCauseAndStop(disruptionKStream);
        complexAggregators.aggregateByTrainAndStop(disruptionKStream);
        complexAggregators.aggregateByTrainAndCause(disruptionKStream);
        complexAggregators.aggregateByStopAndTrainAndCause(disruptionKStream);

        return new KafkaStreams(builder.build(), properties);
    }
}
