package lab4.lab4;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class StreamProcessor {

    private final ObjectMapper mapper = new ObjectMapper();

    @Bean
    public KStream<String, String> process(StreamsBuilder builder) {
        JsonSerde<Trip> tripSerde = new JsonSerde<>(Trip.class);

        KStream<String, String> stream = builder.stream("trips-input", Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, Trip> trips = stream.mapValues(this::parseTrip).filter((k, v) -> v != null);

        // b) Скільки поїздок було здійснено кожного дня
        KTable<String, Long> tripsPerDay = trips.groupBy((k, v) -> v.date, Grouped.with(Serdes.String(), tripSerde)).count();
        tripsPerDay.toStream()
                .mapValues(count -> toJson(Map.of("count", count)))
                .to("trips-count", Produced.with(Serdes.String(), Serdes.String()));

        // a) Середня тривалість поїздки на день
        KTable<String, Long> totalDuration = trips.groupBy((k, v) -> v.date, Grouped.with(Serdes.String(), tripSerde))
                .aggregate(() -> 0L, (date, value, agg) -> agg + value.duration, Materialized.with(Serdes.String(), Serdes.Long()));
        KTable<String, Double> averageDuration = totalDuration.join(tripsPerDay, (sum, count) -> count == 0 ? 0.0 : sum / (double) count);
        averageDuration.toStream()
                .mapValues(avg -> toJson(Map.of("averageDuration", avg)))
                .to("trips-average-duration", Produced.with(Serdes.String(), Serdes.String()));

        // c) Найпопулярніша початкова станція для кожного дня
        KTable<String, Long> startStationCounts = trips.groupBy((k, v) -> v.date + "|" + v.startStation, Grouped.with(Serdes.String(), tripSerde)).count();
        KTable<String, String> topStartStation = startStationCounts.toStream()
                .map((compositeKey, count) -> {
                    String[] parts = compositeKey.split("\\|", 2);
                    return KeyValue.pair(parts[0], toJson(new StationCount(parts[1], count)));
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(this::selectHigherCount);
        topStartStation.toStream()
                .mapValues(json -> {
                    StationCount stationCount = fromJson(json, StationCount.class);
                    return toJson(Map.of("station", stationCount.station(), "count", stationCount.count()));
                })
                .to("top-start-station", Produced.with(Serdes.String(), Serdes.String()));

        // d) Трійка лідерів станцій для поїздок кожного дня (початок і кінець)
        KTable<String, Long> allStationCounts = trips.flatMap((key, trip) -> List.of(
                        KeyValue.pair(trip.date + "|" + trip.startStation, trip.startStation),
                        KeyValue.pair(trip.date + "|" + trip.endStation, trip.endStation)
                ))
                .groupBy((compositeKey, station) -> compositeKey, Grouped.with(Serdes.String(), Serdes.String()))
                .count();

        KTable<String, String> top3Stations = allStationCounts.toStream()
                .map((compositeKey, count) -> {
                    String[] parts = compositeKey.split("\\|", 2);
                    return KeyValue.pair(parts[0], toJson(new StationCount(parts[1], count)));
                })
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "[]",
                        this::updateTop3,
                        Materialized.with(Serdes.String(), Serdes.String())
                );

        top3Stations.toStream().to("top-3-stations", Produced.with(Serdes.String(), Serdes.String()));

        return stream;
    }

    private Trip parseTrip(String value) {
        try {
            return mapper.readValue(value, Trip.class);
        } catch (Exception e) {
            return null;
        }
    }

    private String toJson(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (Exception e) {
            return "{}";
        }
    }

    private <T> T fromJson(String value, Class<T> clazz) {
        try {
            return mapper.readValue(value, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T fromJson(String value, TypeReference<T> reference) {
        try {
            return mapper.readValue(value, reference);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String selectHigherCount(String first, String second) {
        StationCount firstCount = fromJson(first, StationCount.class);
        StationCount secondCount = fromJson(second, StationCount.class);
        return firstCount.count() >= secondCount.count() ? first : second;
    }

    private String updateTop3(String date, String stationJson, String aggJson) {
        StationCount incoming = fromJson(stationJson, StationCount.class);
        List<StationCount> top = fromJson(aggJson, new TypeReference<>() {
        });

        Map<String, StationCount> stationToCount = new LinkedHashMap<>();
        for (StationCount stationCount : top) {
            stationToCount.put(stationCount.station(), stationCount);
        }
        stationToCount.put(incoming.station(), incoming);

        List<StationCount> sorted = new ArrayList<>(stationToCount.values());
        sorted.sort(Comparator.comparingLong(StationCount::count).reversed());
        if (sorted.size() > 3) {
            sorted = sorted.subList(0, 3);
        }
        return toJson(sorted);
    }

    private static record StationCount(String station, long count) {
    }
}
