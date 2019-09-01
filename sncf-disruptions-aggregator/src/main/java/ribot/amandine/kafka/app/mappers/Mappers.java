package ribot.amandine.kafka.app.mappers;

import ribot.amandine.kafka.app.*;

public class Mappers {

    public static KeyDisruptionStop mapToKeyDisruptionStop(Disruption disruption, Integer index) {

        Stop stop = disruption.getStops().get(index);

        return KeyDisruptionStop.newBuilder()
                .setId(disruption.getId())
                .setStopId(stop.getId())
                .build();
    }

    public static DisruptionStop mapToDisruptionStop(Disruption disruption, Integer index) {

        Stop stop = disruption.getStops().get(index);

        return DisruptionStop.newBuilder()
                .setId(stop.getId())
                .setName(stop.getName())
                .setLatitude(stop.getLatitude())
                .setLongitude(stop.getLongitude())
                .setUpdatedAt(disruption.getUpdatedAt())
                .setMessage(disruption.getMessage())
                .build();
    }

    public static KeyDisruptionStopTrain mapToKeyDisruptionStopTrain(Disruption disruption, Integer index) {

        Stop stop = disruption.getStops().get(index);

        return KeyDisruptionStopTrain.newBuilder()
                .setId(disruption.getId())
                .setStopId(stop.getId())
                .setTrainId(disruption.getTrain().getId())
                .build();
    }

    public static DisruptionStopTrain mapToDisruptionStopTrain(Disruption disruption, Integer index) {

        Stop stop = disruption.getStops().get(index);

        return DisruptionStopTrain.newBuilder()
                .setId(stop.getId())
                .setName(stop.getName())
                .setLatitude(stop.getLatitude())
                .setLongitude(stop.getLongitude())
                .setUpdatedAt(disruption.getUpdatedAt())
                .setTrainId(disruption.getTrain().getId())
                .setTrainName(disruption.getTrain().getName())
                .setMessage(disruption.getMessage())
                .build();
    }
}
