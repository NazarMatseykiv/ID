package lab4.lab4;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.TopicBuilder;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamsConfig {

    @Value("${spring.kafka.streams.application-id}")
    private String applicationId;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public NewTopic tripsInputTopic() {
        return TopicBuilder.name("trips-input").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic tripsCountTopic() {
        return TopicBuilder.name("trips-count").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic tripsAverageDurationTopic() {
        return TopicBuilder.name("trips-average-duration").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic topStartStationTopic() {
        return TopicBuilder.name("top-start-station").partitions(1).replicas(1).build();
    }

    @Bean
    public NewTopic top3StationsTopic() {
        return TopicBuilder.name("top-3-stations").partitions(1).replicas(1).build();
    }
}
