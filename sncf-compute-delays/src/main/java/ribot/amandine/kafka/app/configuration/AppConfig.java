package ribot.amandine.kafka.app.configuration;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String uniqueDisruptionTopicName;
    private final String disruptionDelaysTopicName;
    private final String applicationId;



    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.uniqueDisruptionTopicName = config.getString("kafka.unique.disruption.topic.name");
        this.disruptionDelaysTopicName = config.getString("kafka.disruption.delays.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
    }

    public String getBootstrapServers() { return bootstrapServers; }

    public String getSchemaRegistryUrl() { return schemaRegistryUrl; }

    public String getUniqueDisruptionTopicName() { return uniqueDisruptionTopicName; }

    public String getDisruptionDelaysTopicName() { return disruptionDelaysTopicName; }

    public String getApplicationId() { return applicationId; }
}
