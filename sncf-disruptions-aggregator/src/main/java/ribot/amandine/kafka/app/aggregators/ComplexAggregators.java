package ribot.amandine.kafka.app.aggregators;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import ribot.amandine.kafka.app.*;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static ribot.amandine.kafka.app.mappers.Mappers.*;

public class ComplexAggregators {

    private final AppConfig appConfig;

    public ComplexAggregators() {

        appConfig = new AppConfig(ConfigFactory.load());
    }

    public void aggregateByCauseAndStop(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET AGGREGATION OF DISRUPTIONS PER STOP ID AND CAUSE

        SpecificAvroSerde<StopCauseStatistic> stopCauseStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<KeyStopCause> keyStopCauseSpecificAvroSerde = new SpecificAvroSerde<>();
        stopCauseStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        keyStopCauseSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyDisruptionStop, DisruptionStop> disruptionStreamPerStopName = disruptionKStream
                .flatMap((key, value) -> {
                    List<KeyValue<KeyDisruptionStop, DisruptionStop>> results = new ArrayList<>();

                    for (int i = 0; i < value.getStops().size(); i++) {
                        results.add(new KeyValue<>(mapToKeyDisruptionStop(value, i), mapToDisruptionStop(value, i)));
                    }

                    return results;
                });


        KTable<KeyStopCause, StopCauseStatistic> stopCauseStatisticsKTable = disruptionStreamPerStopName
                .selectKey((key, value) -> KeyStopCause.newBuilder().setName(value.getName()).setMessage(value.getMessage()).build())
                .groupByKey()
                .aggregate(
                        this::initStopCauseStatistic,
                        this::aggregateStopCauseStatistic,
                        Materialized.<KeyStopCause, StopCauseStatistic, KeyValueStore<Bytes, byte[]>>as("stop-cause-statistic-table-store")
                                .withKeySerde(keyStopCauseSpecificAvroSerde)
                                .withValueSerde(stopCauseStatisticSpecificAvroSerde));

        stopCauseStatisticsKTable
                .toStream()
                .to(appConfig.getStopCauseStatsTopicName(), Produced.with(keyStopCauseSpecificAvroSerde, stopCauseStatisticSpecificAvroSerde));

    }

    private StopCauseStatistic initStopCauseStatistic() {

        return StopCauseStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private StopCauseStatistic aggregateStopCauseStatistic(KeyStopCause key, DisruptionStop disruptionStop, StopCauseStatistic currentStats) {

        return StopCauseStatistic.newBuilder(currentStats)
                .setId(disruptionStop.getId())
                .setName(disruptionStop.getName())
                .setLatitude(disruptionStop.getLatitude())
                .setLongitude(disruptionStop.getLongitude())
                .setText(disruptionStop.getMessage())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    public void aggregateByTrainAndStop(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET AGGREGATION OF DISRUPTIONS PER STOP ID AND TRAIN

        SpecificAvroSerde<StopTrainStatistic> stopTrainStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<KeyStopTrain> keyStopTrainSpecificAvroSerde = new SpecificAvroSerde<>();
        stopTrainStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        keyStopTrainSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyDisruptionStopTrain, DisruptionStopTrain> disruptionStreamPerStopName = disruptionKStream
                .flatMap((key, value) -> {
                    List<KeyValue<KeyDisruptionStopTrain, DisruptionStopTrain>> results = new ArrayList<>();

                    for (int i = 0; i < value.getStops().size(); i++) {
                        results.add(new KeyValue<>(mapToKeyDisruptionStopTrain(value, i), mapToDisruptionStopTrain(value, i)));
                    }

                    return results;
                });

        KTable<KeyStopTrain, StopTrainStatistic> stopTrainStatisticsKTable = disruptionStreamPerStopName
                .selectKey((key, value) -> KeyStopTrain.newBuilder().setName(value.getName()).setTrainName(value.getTrainName()).build())
                .groupByKey()
                .aggregate(
                        this::initStopTrainStatistic,
                        this::aggregateStopTrainStatistic,
                        Materialized.<KeyStopTrain, StopTrainStatistic, KeyValueStore<Bytes, byte[]>>as("stop-train-statistic-table-store")
                                .withKeySerde(keyStopTrainSpecificAvroSerde)
                                .withValueSerde(stopTrainStatisticSpecificAvroSerde));

        stopTrainStatisticsKTable
                .toStream()
                .to(appConfig.getStopTrainStatsTopicName(), Produced.with(keyStopTrainSpecificAvroSerde, stopTrainStatisticSpecificAvroSerde));

    }

    private StopTrainStatistic initStopTrainStatistic() {

        return StopTrainStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private StopTrainStatistic aggregateStopTrainStatistic(KeyStopTrain stopId, DisruptionStopTrain disruptionStopTrain, StopTrainStatistic currentStats) {

        return StopTrainStatistic.newBuilder(currentStats)
                .setId(disruptionStopTrain.getId())
                .setName(disruptionStopTrain.getName())
                .setLatitude(disruptionStopTrain.getLatitude())
                .setLongitude(disruptionStopTrain.getLongitude())
                .setTrainId(disruptionStopTrain.getTrainId())
                .setTrainName(disruptionStopTrain.getTrainName())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    public void aggregateByTrainAndCause(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET AGGREGATION OF DISRUPTIONS PER TRAIN AND CAUSE

        SpecificAvroSerde<TrainCauseStatistic> trainCauseStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<KeyTrainCause> keyTrainCauseSpecificAvroSerde = new SpecificAvroSerde<>();
        trainCauseStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        keyTrainCauseSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);


        KTable<KeyTrainCause, TrainCauseStatistic> trainCauseStatisticsKTable = disruptionKStream
                .selectKey((key, value) -> KeyTrainCause.newBuilder().setTrainName(value.getTrain().getName()).setMessage(value.getMessage()).build())
                .groupByKey()
                .aggregate(
                        this::initTrainCauseStatistic,
                        this::aggregateTrainCauseStatistic,
                        Materialized.<KeyTrainCause, TrainCauseStatistic, KeyValueStore<Bytes, byte[]>>as("train-cause-statistic-table-store")
                                .withKeySerde(keyTrainCauseSpecificAvroSerde)
                                .withValueSerde(trainCauseStatisticSpecificAvroSerde));

        trainCauseStatisticsKTable
                .toStream()
                .to(appConfig.getTrainCauseStatsTopicName(), Produced.with(keyTrainCauseSpecificAvroSerde, trainCauseStatisticSpecificAvroSerde));

    }

    private TrainCauseStatistic initTrainCauseStatistic() {

        return TrainCauseStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private TrainCauseStatistic aggregateTrainCauseStatistic(KeyTrainCause id, Disruption disruption, TrainCauseStatistic currentStats) {

        return TrainCauseStatistic.newBuilder(currentStats)
                .setTrainId(disruption.getTrain().getId())
                .setTrainName(disruption.getTrain().getName())
                .setText(disruption.getMessage())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    public void aggregateByStopAndTrainAndCause(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET AGGREGATION OF DISRUPTIONS PER STOP AND TRAIN AND CAUSE

        SpecificAvroSerde<StopTrainCauseStatistic> stopTrainCauseStatisticSpecificAvroSerde = new SpecificAvroSerde<>();
        SpecificAvroSerde<KeyStopTrainCause> keyStopTrainCauseSpecificAvroSerde = new SpecificAvroSerde<>();
        stopTrainCauseStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        keyStopTrainCauseSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyDisruptionStopTrain, DisruptionStopTrain> disruptionStreamPerStopName = disruptionKStream
                .flatMap((key, value) -> {
                    List<KeyValue<KeyDisruptionStopTrain, DisruptionStopTrain>> results = new ArrayList<>();

                    for (int i = 0; i < value.getStops().size(); i++) {
                        results.add(new KeyValue<>(mapToKeyDisruptionStopTrain(value, i), mapToDisruptionStopTrain(value, i)));
                    }

                    return results;
                });

        KTable<KeyStopTrainCause, StopTrainCauseStatistic> stopTrainCauseStatisticsKTable = disruptionStreamPerStopName
                .selectKey((key, value) -> KeyStopTrainCause.newBuilder()
                                                            .setMessage(value.getMessage())
                                                            .setName(value.getName())
                                                            .setTrainName(value.getTrainName())
                                                            .build())
                .groupByKey()
                .aggregate(
                        this::initStopTrainCauseStatistic,
                        this::aggregateStopTrainCauseStatistic,
                        Materialized.<KeyStopTrainCause, StopTrainCauseStatistic, KeyValueStore<Bytes, byte[]>>as("stop-train-cause-statistic-table-store")
                                .withKeySerde(keyStopTrainCauseSpecificAvroSerde)
                                .withValueSerde(stopTrainCauseStatisticSpecificAvroSerde));

        stopTrainCauseStatisticsKTable
                .toStream()
                .to(appConfig.getStopTrainCauseStatsTopicName(), Produced.with(keyStopTrainCauseSpecificAvroSerde, stopTrainCauseStatisticSpecificAvroSerde));

    }

    private StopTrainCauseStatistic initStopTrainCauseStatistic() {

        return StopTrainCauseStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private StopTrainCauseStatistic aggregateStopTrainCauseStatistic(KeyStopTrainCause id, DisruptionStopTrain disruptionStopTrain, StopTrainCauseStatistic currentStats) {

        return StopTrainCauseStatistic.newBuilder(currentStats)
                .setId(disruptionStopTrain.getId())
                .setName(disruptionStopTrain.getName())
                .setLatitude(disruptionStopTrain.getLatitude())
                .setLongitude(disruptionStopTrain.getLongitude())
                .setTrainId(disruptionStopTrain.getTrainId())
                .setTrainName(disruptionStopTrain.getTrainName())
                .setText(disruptionStopTrain.getMessage())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }
}
