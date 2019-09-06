package ribot.amandine.kafka.app.topology;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.joda.time.DateTime;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import ribot.amandine.kafka.app.*;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ComputeDelaysTopology {

    private final AppConfig appConfig;
    private final SpecificAvroSerde<KeyDisruption> keyDisruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private final SpecificAvroSerde<Disruption> disruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private final DateFormat timeFormat = new SimpleDateFormat("hhmmss");

    public ComputeDelaysTopology(Properties properties) {

        appConfig = new AppConfig(ConfigFactory.load());
        KafkaStreams streams = createTopology(properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private KafkaStreams createTopology(Properties properties) {

        keyDisruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), true);
        disruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<KeyDisruption, Disruption> disruptionKStream = builder
                .stream(appConfig.getUniqueDisruptionTopicName(), Consumed.with(keyDisruptionSpecificAvroSerde, disruptionSpecificAvroSerde))
                .peek(((key, value) -> System.out.println(value.toString())))
                .filter((key, value) -> key != null && value != null);

        disruptionKStream
                .mapValues((value) -> mapToDisruptionDelays((value)))
                .to(appConfig.getDisruptionDelaysTopicName());

        return new KafkaStreams(builder.build(), properties);
    }

    private DisruptionDelays mapToDisruptionDelays(Disruption disruption) {

        List<Stop> stops = disruption.getStops();
        List<StopDelays> stopDelays = new ArrayList<>();
        List<CharSequence> delays = new ArrayList<>();
        List<CharSequence> causes = new ArrayList<>();

        for (Stop stop : stops) {

            StopDelays stopDelay = StopDelays.newBuilder()
                    .setId(stop.getId())
                    .setName(stop.getName())
                    .setLatitude(stop.getLatitude())
                    .setLongitude(stop.getLongitude())
                    .build();

            stopDelays.add(stopDelay);
            causes.add(stop.getTimes().getCause().toString());

            try {
                delays.add(calculateDelays(stop.getTimes()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        return DisruptionDelays.newBuilder()
                .setId(disruption.getId())
                .setTrainId(disruption.getTrain().getId())
                .setTrainName(disruption.getTrain().getName())
                .setMessage(disruption.getMessage())
                .setUpdatedAt(disruption.getUpdatedAt())
                .setStops(stopDelays)
                .setDelays(delays)
                .setCauses(causes)
                .build();
    }

    private String calculateDelays(Information timeInformation) throws ParseException {

        String calculatedDelay;
        int calculatedDelayArrival = 0;
        int calculatedDelayDeparture = 0;

        if(timeInformation.getBaseArrivalTime() != null && timeInformation.getNewArrivalTime() != null) {

            DateTime plannedArrivalToStation = new DateTime(timeFormat.parse(timeInformation.getBaseArrivalTime().toString()));
            DateTime newArrivalToStation = new DateTime(timeFormat.parse(timeInformation.getNewArrivalTime().toString()));
            calculatedDelayArrival = Minutes.minutesBetween(plannedArrivalToStation, newArrivalToStation).getMinutes() % 60;

        }

        if(timeInformation.getBaseDepartureTime() != null && timeInformation.getNewDepartureTime() != null) {
            DateTime plannedDepartureOfStation = new DateTime(timeFormat.parse(timeInformation.getBaseDepartureTime().toString()));
            DateTime newDepartureOfStation = new DateTime(timeFormat.parse(timeInformation.getNewDepartureTime().toString()));
            calculatedDelayDeparture = Minutes.minutesBetween(plannedDepartureOfStation, newDepartureOfStation).getMinutes() % 60;
        }

        if(calculatedDelayDeparture >= calculatedDelayArrival) {
            calculatedDelay = Integer.toString(calculatedDelayDeparture);
        }
        else {
            calculatedDelay = Integer.toString(calculatedDelayArrival);
        }

        return calculatedDelay;
    }
}
