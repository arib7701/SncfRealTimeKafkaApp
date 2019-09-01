package ribot.amandine.kafka.app.configuration;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String disruptionTopicName;
    private final String applicationId;

    // FOR SIMPLE STATS
    private final String stopStatsTopicName;
    private final String stopStatsPerDayTopicName;
    private final String trainStatsTopicName;
    private final String trainStatsPerDayTopicName;
    private final String causeStatsTopicName;
    private final String causeStatsPerDayTopicName;

    // FOR COMPLEX STATS
    private final String stopCauseStatsTopicName;
    private final String stopTrainStatsTopicName;
    private final String trainCauseStatsTopicName;
    private final String stopTrainCauseStatsTopicName;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.disruptionTopicName = config.getString("kafka.disruption.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
        this.stopStatsTopicName = config.getString("kafka.stop.stats.topic.name");
        this.stopStatsPerDayTopicName = config.getString("kafka.stop.stats.per.day.topic.name");
        this.trainStatsTopicName = config.getString("kafka.train.stats.topic.name");
        this.trainStatsPerDayTopicName = config.getString("kafka.train.stats.per.day.topic.name");
        this.causeStatsTopicName = config.getString("kafka.cause.stats.topic.name");
        this.causeStatsPerDayTopicName = config.getString("kafka.cause.stats.per.day.topic.name");
        this.stopCauseStatsTopicName = config.getString("kafka.stop.cause.stats.topic.name");
        this.stopTrainStatsTopicName = config.getString("kafka.stop.train.stats.topic.name");
        this.trainCauseStatsTopicName = config.getString("kafka.train.cause.stats.topic.name");
        this.stopTrainCauseStatsTopicName = config.getString("kafka.stop.train.cause.stats.topic.name");
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getDisruptionTopicName() {
        return disruptionTopicName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getStopStatsTopicName() {
        return stopStatsTopicName;
    }

    public String getStopStatsPerDayTopicName() { return stopStatsPerDayTopicName; }

    public String getTrainStatsTopicName() { return trainStatsTopicName; }

    public String getTrainStatsPerDayTopicName() { return trainStatsPerDayTopicName; }

    public String getCauseStatsTopicName() { return causeStatsTopicName; }

    public String getCauseStatsPerDayTopicName() { return causeStatsPerDayTopicName; }

    public String getStopCauseStatsTopicName() { return stopCauseStatsTopicName; }

    public String getStopTrainStatsTopicName() { return stopTrainStatsTopicName; }

    public String getTrainCauseStatsTopicName() { return trainCauseStatsTopicName; }

    public String getStopTrainCauseStatsTopicName() { return stopTrainCauseStatsTopicName; }

}
