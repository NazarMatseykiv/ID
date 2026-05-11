package lab4.lab4;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Component
public class TopicInitializer {

    private static final Logger log = LoggerFactory.getLogger(TopicInitializer.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @PostConstruct
    public void createTopics() {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {
            List<NewTopic> topics = List.of(
                    new NewTopic("trips-input", 1, (short) 1),
                    new NewTopic("trips-count", 1, (short) 1),
                    new NewTopic("trips-average-duration", 1, (short) 1),
                    new NewTopic("top-start-station", 1, (short) 1),
                    new NewTopic("top-3-stations", 1, (short) 1)
            );

            adminClient.createTopics(topics).all().get(Duration.ofSeconds(30).toMillis(), java.util.concurrent.TimeUnit.MILLISECONDS);
            log.info("Kafka topics created or already exist: {}", topics.stream().map(NewTopic::name).toList());
        } catch (ExecutionException e) {
            if (e.getCause() instanceof TopicExistsException) {
                log.info("Kafka topics already exist");
            } else {
                log.error("Failed to create Kafka topics", e);
                throw new IllegalStateException("Kafka topic creation failed", e);
            }
        } catch (Exception e) {
            log.error("Failed to create Kafka topics", e);
            throw new IllegalStateException("Kafka topic creation failed", e);
        }
    }
}
