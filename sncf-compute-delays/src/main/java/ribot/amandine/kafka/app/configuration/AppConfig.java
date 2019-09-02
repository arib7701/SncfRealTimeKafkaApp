package ribot.amandine.kafka.app.configuration;

import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String disruptionTopicName;
    private final String applicationId;



    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.disruptionTopicName = config.getString("kafka.disruption.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
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
}
