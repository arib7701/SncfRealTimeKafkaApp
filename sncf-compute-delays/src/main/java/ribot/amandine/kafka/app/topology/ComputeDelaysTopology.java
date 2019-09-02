package ribot.amandine.kafka.app.topology;

import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import ribot.amandine.kafka.app.*;
import ribot.amandine.kafka.app.configuration.AppConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class ComputeDelaysTopology {

    private final AppConfig appConfig;
    private final SpecificAvroSerde<Disruption> disruptionSpecificAvroSerde = new SpecificAvroSerde<>();
    private final SpecificAvroSerde<KeyDisruption> keyDisruptionSpecificAvroSerde = new SpecificAvroSerde<>();

    public ComputeDelaysTopology(Properties properties) {

        appConfig = new AppConfig(ConfigFactory.load());
        KafkaStreams streams = createTopology(properties);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private KafkaStreams createTopology(Properties properties) {

        disruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);
        keyDisruptionSpecificAvroSerde.configure(Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, appConfig.getSchemaRegistryUrl()), false);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<KeyDisruption, Disruption> disruptionKStream = builder
                .stream(appConfig.getDisruptionTopicName(), Consumed.with(keyDisruptionSpecificAvroSerde, disruptionSpecificAvroSerde))
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
            delays.add(calculateDelays(stop.getTimes()));
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

    private String calculateDelays(Information timeInformation) {

        String calculatedDelay;
        int calculatedDelayArrival;
        int calculatedDelayDeparture;

        String plannedArrivalToStation = timeInformation.getBaseArrivalTime().toString();
        String newArrivalToStation = timeInformation.getNewArrivalTime().toString();

        String plannedDepartureOfStation = timeInformation.getBaseDepartureTime().toString();
        String newDepartureOfStation = timeInformation.getNewDepartureTime().toString();


        if(plannedArrivalToStation == "" || newArrivalToStation == "" || plannedArrivalToStation.equals(newArrivalToStation) ) {
            calculatedDelayArrival = 0;
        } else {
            calculatedDelayArrival = Integer.parseInt(plannedArrivalToStation) - Integer.parseInt(newArrivalToStation);
        }

        if(plannedDepartureOfStation == "" || newDepartureOfStation == "" || plannedDepartureOfStation.equals(newDepartureOfStation)) {
            calculatedDelayDeparture = 0;
        } else {
            calculatedDelayDeparture = Integer.parseInt(plannedDepartureOfStation) - Integer.parseInt(newDepartureOfStation);
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
