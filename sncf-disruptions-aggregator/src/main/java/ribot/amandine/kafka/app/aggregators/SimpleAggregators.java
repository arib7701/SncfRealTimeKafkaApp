package ribot.amandine.kafka.app.aggregators;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import ribot.amandine.kafka.app.*;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class SimpleAggregators {

    private final AppConfig appConfig;

    public SimpleAggregators() {

        appConfig = new AppConfig(ConfigFactory.load());
    }

    public void aggregateStopFromStream(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET STATISTIC OF DISRUPTIONS PER STOP ID
        final SpecificAvroSerde<KeyStop> keyStopSpecificAvroSerde = new SpecificAvroSerde<>();
        final SpecificAvroSerde<StopStatistic> stopStatisticSpecificAvroSerde = new SpecificAvroSerde<>();

        keyStopSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), true);
        stopStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyStop, Stop> disruptionStreamPerStopName = disruptionKStream
                .flatMap((key, value) -> {
                    List<KeyValue<KeyStop, Stop>> results = new ArrayList<>();

                    for (int i = 0; i < value.getStops().size(); i++) {
                        results.add(new KeyValue<>(KeyStop.newBuilder().setId(value.getStops().get(i).getId()).build(), value.getStops().get(i)));
                    }

                    return results;
                });


        KTable<KeyStop, StopStatistic> stopStatisticsKTable = disruptionStreamPerStopName
                .groupByKey()
                .aggregate(
                        this::initStopStatistic,
                        this::aggregateStopStatistic,
                        Materialized.<KeyStop, StopStatistic, KeyValueStore<Bytes, byte[]>>as("stop-statistic-table-store")
                                .withKeySerde(keyStopSpecificAvroSerde)
                                .withValueSerde(stopStatisticSpecificAvroSerde));

        stopStatisticsKTable
                .toStream()
                .to(appConfig.getStopStatsTopicName(), Produced.with(keyStopSpecificAvroSerde, stopStatisticSpecificAvroSerde));


        // GET STATISTIC OF DISRUPTIONS PER STOP ID PER DAY

        Duration windowSizeDuration = Duration.ofDays(1);
        TimeWindows timeWindows = TimeWindows.of(windowSizeDuration);

        KTable<Windowed<KeyStop>, StopStatistic> stopStatisticsKTablePerDay = disruptionStreamPerStopName
                .groupByKey()
                .windowedBy(timeWindows)
                .aggregate(
                        this::initStopStatistic,
                        this::aggregateStopStatistic,
                        Materialized.<KeyStop, StopStatistic, WindowStore<Bytes, byte[]>>as("stop-statistic-per-day-table-store")
                                .withKeySerde(keyStopSpecificAvroSerde)
                                .withValueSerde(stopStatisticSpecificAvroSerde));

        stopStatisticsKTablePerDay
                .toStream()
                .to(appConfig.getStopStatsPerDayTopicName());
    }


    private StopStatistic initStopStatistic() {

        return StopStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private StopStatistic aggregateStopStatistic(KeyStop stopId, Stop stop, StopStatistic currentStats) {

        return StopStatistic.newBuilder(currentStats)
                .setId(stop.getId())
                .setName(stop.getName())
                .setLatitude(stop.getLatitude())
                .setLongitude(stop.getLongitude())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    public void aggregateTrainFromStream(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET STATISTIC OF DISRUPTIONS PER TRAIN ID
        final SpecificAvroSerde<KeyTrain> keyTrainSpecificAvroSerde = new SpecificAvroSerde<>();
        final SpecificAvroSerde<TrainStatistic> trainStatisticSpecificAvroSerde = new SpecificAvroSerde<>();

        keyTrainSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), true);
        trainStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyTrain, Train> disruptionStreamPerTrainName = disruptionKStream
                .map((key, value) -> KeyValue.pair(KeyTrain.newBuilder().setId(value.getTrain().getId()).build(), value.getTrain()));


        KTable<KeyTrain, TrainStatistic> trainStatisticKTable = disruptionStreamPerTrainName
                .groupByKey()
                .aggregate(
                        this::initTrainStatistic,
                        this::aggregateTrainStatistic,
                        Materialized.<KeyTrain, TrainStatistic, KeyValueStore<Bytes, byte[]>>as("train-statistic-table-store")
                                .withKeySerde(keyTrainSpecificAvroSerde)
                                .withValueSerde(trainStatisticSpecificAvroSerde));

        trainStatisticKTable
                .toStream()
                .to(appConfig.getTrainStatsTopicName(), Produced.with(keyTrainSpecificAvroSerde, trainStatisticSpecificAvroSerde));

        // GET STATISTIC OF DISRUPTIONS PER TRAIN ID PER DAY

        Duration windowSizeDuration = Duration.ofDays(1);
        TimeWindows timeWindows = TimeWindows.of(windowSizeDuration);

        KTable<Windowed<KeyTrain>, TrainStatistic> trainStatisticKTablePerDay = disruptionStreamPerTrainName
                .groupByKey()
                .windowedBy(timeWindows)
                .aggregate(
                        this::initTrainStatistic,
                        this::aggregateTrainStatistic,
                        Materialized.<KeyTrain, TrainStatistic, WindowStore<Bytes, byte[]>>as("train-statistic-per-day-table-store")
                                .withKeySerde(keyTrainSpecificAvroSerde)
                                .withValueSerde(trainStatisticSpecificAvroSerde));

        trainStatisticKTablePerDay
                .toStream()
                .to(appConfig.getTrainStatsPerDayTopicName());

    }

    private TrainStatistic initTrainStatistic() {

        return TrainStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private TrainStatistic aggregateTrainStatistic(KeyTrain trainId, Train train, TrainStatistic currentStats) {

        return TrainStatistic.newBuilder(currentStats)
                .setId(train.getId())
                .setName(train.getName())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }

    public void aggregateCauseFromStream(KStream<KeyDisruption, Disruption> disruptionKStream) {

        // GET STATISTIC OF DISRUPTIONS PER CAUSE
        final SpecificAvroSerde<KeyCause> keyCauseSpecificAvroSerde = new SpecificAvroSerde<>();
        final SpecificAvroSerde<CauseStatistic> causeStatisticSpecificAvroSerde = new SpecificAvroSerde<>();

        keyCauseSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), true);
        causeStatisticSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        KStream<KeyCause, Disruption> disruptionStreamPerCause = disruptionKStream
                .filter((key, value) -> value.getMessage() != null)
                .map((key, value) -> KeyValue.pair(KeyCause.newBuilder().setText(value.getMessage()).build(), value));


        KTable<KeyCause, CauseStatistic> causeStatisticsKTable = disruptionStreamPerCause
                .groupByKey()
                .aggregate(
                        this::initCauseStatistic,
                        this::aggregateCauseStatistic,
                        Materialized.<KeyCause, CauseStatistic, KeyValueStore<Bytes, byte[]>>as("cause-statistic-table-store")
                                .withKeySerde(keyCauseSpecificAvroSerde)
                                .withValueSerde(causeStatisticSpecificAvroSerde));

        causeStatisticsKTable
                .toStream()
                .to(appConfig.getCauseStatsTopicName(), Produced.with(keyCauseSpecificAvroSerde, causeStatisticSpecificAvroSerde));

        // GET STATISTIC OF DISRUPTIONS PER CAUSE ID PER DAY

        Duration windowSizeDuration = Duration.ofDays(1);
        TimeWindows timeWindows = TimeWindows.of(windowSizeDuration);

        KTable<Windowed<KeyCause>, CauseStatistic> causeStatisticsKTablePerDay = disruptionStreamPerCause
                .groupByKey()
                .windowedBy(timeWindows)
                .aggregate(
                        this::initCauseStatistic,
                        this::aggregateCauseStatistic,
                        Materialized.<KeyCause, CauseStatistic, WindowStore<Bytes, byte[]>>as("cause-statistic-per-day-table-store")
                                .withKeySerde(keyCauseSpecificAvroSerde)
                                .withValueSerde(causeStatisticSpecificAvroSerde));

        causeStatisticsKTablePerDay
                .toStream()
                .to(appConfig.getCauseStatsPerDayTopicName());

    }

    private CauseStatistic initCauseStatistic() {

        return CauseStatistic.newBuilder()
                .setNumberDisruption(0)
                .build();
    }

    private CauseStatistic aggregateCauseStatistic(KeyCause text, Disruption disruption, CauseStatistic currentStats) {

        return CauseStatistic.newBuilder(currentStats)
                .setText(disruption.getMessage())
                .setUpdated(new Date().toString())
                .setNumberDisruption(currentStats.getNumberDisruption()+1)
                .build();
    }
}
