package lab4.lab4;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class StreamProcessor {

    private final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> process(StreamsBuilder builder) {

        KStream<String, String> stream = builder.stream("trips-input");

        KStream<String, Trip> trips = stream.mapValues(value -> {
            try {
                return mapper.readValue(value, Trip.class);
            } catch (Exception e) {
                return null;
            }
        }).filter((k, v) -> v != null);

        // 1. Кількість поїздок
        trips.groupBy((k, v) -> v.date)
                .count()
                .toStream()
                .to("trips-count", Produced.with(Serdes.String(), Serdes.Long()));

        return stream;
    }
}