package ribot.amandine.kafka.app.topology;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import ribot.amandine.kafka.app.*;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class DisruptionsAggregatorTopology {

    private final AppConfig appConfig;

    public DisruptionsAggregatorTopology(Properties properties) {

        appConfig = new AppConfig(ConfigFactory.load());
        KafkaStreams streams = createTopology(properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private KafkaStreams createTopology(Properties properties) {

        SpecificAvroSerde<Disruption> disruptionSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<StopStatistic> stopStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<TrainStatistic> trainStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<CauseStatistic> causeStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        Serdes.StringSerde stringSerde = new Serdes.StringSerde();

        disruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        stopStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        trainStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        causeStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Disruption> disruptionKStream = builder.stream(appConfig.getDisruptionTopicName(), Consumed.with(Serdes.String(), disruptionSpecificAvroSerde));


        // GET STATISTIC OF DISRUPTIONS PER STOP ID

        KStream<String, Stop> disruptionStreamPerStopName = disruptionKStream
                .flatMap((key, value) -> {
                    List<KeyValue<String, Stop>> results = new ArrayList<>();

                    for (int i = 0; i < value.getStops().size(); i++) {
                        results.add(new KeyValue<>(value.getStops().get(i).getId().toString(), value.getStops().get(i)));
                    }

                    return results;
                });


        KTable<String, StopStatistic> stopStatisticsKTable = disruptionStreamPerStopName
                .groupByKey()
                .aggregate(
                        this::initStopStatistic,
                        this::aggregateStopStatistic,
                        Materialized.<String, StopStatistic, KeyValueStore<Bytes, byte[]>>as("stop-statistic-table-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(stopStatisticSpecificAvroSerde));

        stopStatisticsKTable
                .toStream()
                .to(appConfig.getStopStatsTopicName(), Produced.with(stringSerde, stopStatisticSpecificAvroSerde));


        // GET STATISTIC OF DISRUPTIONS PER TRAIN ID

        KStream<String, Train> disruptionStreamPerTrainName = disruptionKStream
                .map((key, value) -> KeyValue.pair(value.getTrain().getId().toString(), value.getTrain()));


        KTable<String, TrainStatistic> trainStatisticKTable = disruptionStreamPerTrainName
                .groupByKey()
                .aggregate(
                        this::initTrainStatistic,
                        this::aggregateTrainStatistic,
                        Materialized.<String, TrainStatistic, KeyValueStore<Bytes, byte[]>>as("train-statistic-table-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(trainStatisticSpecificAvroSerde));

        trainStatisticKTable
                .toStream()
                .to(appConfig.getTrainStatsTopicName(), Produced.with(stringSerde, trainStatisticSpecificAvroSerde));


        // GET STATISTIC OF DISRUPTIONS PER CAUSE

        KStream<String, Disruption> disruptionStreamPerCause = disruptionKStream
                .filter((key, value) -> value.getMessage() != null)
                .map((key, value) -> KeyValue.pair(value.getMessage().toString(), value));


        KTable<String, CauseStatistic> causeStatisticsKTable = disruptionStreamPerCause
                .groupByKey()
                .aggregate(
                        this::initCauseStatistic,
                        this::aggregateCauseStatistic,
                        Materialized.<String, CauseStatistic, KeyValueStore<Bytes, byte[]>>as("cause-statistic-table-store")
                                .withKeySerde(stringSerde)
                                .withValueSerde(causeStatisticSpecificAvroSerde));

        causeStatisticsKTable
                .toStream()
                .to(appConfig.getCauseStatsTopicName(), Produced.with(stringSerde, causeStatisticSpecificAvroSerde));

        return new KafkaStreams(builder.build(), properties);
    }

    private StopStatistic initStopStatistic() {

        return StopStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private StopStatistic aggregateStopStatistic(String stopId, Stop stop, StopStatistic currentStats) {

        return StopStatistic.newBuilder(currentStats)
                .setId(stopId)
                .setName(stop.getName())
                .setLatitude(stop.getLatitude())
                .setLongitude(stop.getLongitude())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    private TrainStatistic initTrainStatistic() {

        return TrainStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private TrainStatistic aggregateTrainStatistic(String trainId, Train train, TrainStatistic currentStats) {

        return TrainStatistic.newBuilder(currentStats)
                .setId(trainId)
                .setName(train.getName())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    private CauseStatistic initCauseStatistic() {

        return CauseStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private CauseStatistic aggregateCauseStatistic(String text, Disruption disruption, CauseStatistic currentStats) {

        return CauseStatistic.newBuilder(currentStats)
                .setText(text)
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }
}
