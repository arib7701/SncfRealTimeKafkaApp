package ribot.amandine.kafka.app.configuration;


import com.typesafe.config.Config;

public class AppConfig {

    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String topicName;
    private final Integer queueCapacity;
    private final Integer producerFrequencyMs;
    private final Integer pageSize;
    private final String username;
    private final String password;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
        this.topicName = config.getString("kafka.topic.name");
        this.queueCapacity = config.getInt("app.queue.capacity");
        this.producerFrequencyMs = config.getInt("app.producer.frequency.ms");
        this.pageSize = config.getInt("app.page.size");
        this.username = config.getString("sncf.username");
        this.password = config.getString("sncf.password");
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public String getTopicName() {
        return topicName;
    }

    public Integer getQueueCapacity() {
        return queueCapacity;
    }

    public Integer getProducerFrequencyMs() {
        return producerFrequencyMs;
    }

    public Integer getPageSize() {
        return pageSize;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
}
