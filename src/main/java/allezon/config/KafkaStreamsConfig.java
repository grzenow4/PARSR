package allezon.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.aerospike.client.AerospikeClient;

import allezon.domain.AggregatedValue;
import allezon.domain.UserTagEvent;
import allezon.service.AerospikeService;

import static allezon.constant.Constants.AGGREGATION_WINDOW_SIZE;
import static allezon.constant.Constants.ANALYTICS_INPUT_TOPIC;
import static allezon.constant.Constants.APP_ID;
import static allezon.constant.Constants.BLANK;
import static allezon.constant.Constants.DELIMITER;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Configuration
public class KafkaStreamsConfig {                    
    public static final DateTimeFormatter BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);

    @Value("${spring.kafka.streams.bootstrap-servers}")
    private String bootstrapServers;

    private final AerospikeService aerospikeService;

    @Autowired
    public KafkaStreamsConfig(AerospikeService aerospikeService) {
        this.aerospikeService = aerospikeService;
    }

    @Bean
    public Properties kafkaStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        String stateDir = "/tmp/kafka-streams/" + System.getenv("HOSTNAME") + "allezon";
        System.out.println("set state dir to: " + stateDir);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 10);  // Ensures matching partitions
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        return props;
    }

    @Bean
    public KafkaStreams kafkaStreams(
            StreamsBuilder builder,
            Properties kafkaStreamsProperties,
            AerospikeClient client,
            JsonSerde<UserTagEvent> userTagEventSerde,
            JsonSerde<AggregatedValue> aggregatedValueSerde) {
        KStream<String, UserTagEvent> userTagEventsStream = builder.stream(
                ANALYTICS_INPUT_TOPIC,
                Consumed.with(Serdes.String(), userTagEventSerde)
        );
    
        KTable<Windowed<String>, AggregatedValue> aggregatedTable = userTagEventsStream
            .flatMap((key, userTagEvent) -> reKeyInputStream(userTagEvent))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(AGGREGATION_WINDOW_SIZE), Duration.ZERO))
            .aggregate(
                AggregatedValue::new,
                (key, event, aggregate) -> aggregate.aggregateProduct(event),
                Materialized.with(Serdes.String(), aggregatedValueSerde) 
        )
        .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())); // Suppress until the window closes

        aggregatedTable.toStream().foreach((key, value) -> {
            boolean success = false;
            while (!success) {
                success = aerospikeService.writeAggregatesToAerospike(key.key(), value);
            }
        });
    
        KafkaStreams streams = new KafkaStreams(builder.build(), kafkaStreamsProperties);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close(Duration.ofSeconds(10));
            streams.cleanUp();
        }));
        
        streams.start();
        return streams;
    }

    private List<KeyValue<String, Integer>> reKeyInputStream(UserTagEvent userTagEvent) {
        String timeBucket = generate1MinuteBucket(userTagEvent.getTime());
        String action = userTagEvent.getAction().toString();
        String origin = userTagEvent.getOrigin();
        String brandId = userTagEvent.getProductInfo().getBrandId();
        String categoryId = userTagEvent.getProductInfo().getCategoryId();
        int price = userTagEvent.getProductInfo().getPrice();
        // Re-key the stream for easier querying
        List<KeyValue<String, Integer>> records = Arrays.asList(
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + origin + DELIMITER + brandId + DELIMITER + categoryId, price),

            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + BLANK + DELIMITER + brandId + DELIMITER + categoryId, price),
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + origin + DELIMITER + BLANK + DELIMITER + categoryId, price),
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + origin + DELIMITER + brandId + DELIMITER + BLANK, price),

            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + BLANK + DELIMITER + BLANK + DELIMITER + categoryId, price),
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + BLANK + DELIMITER + brandId + DELIMITER + BLANK, price),
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + origin + DELIMITER + BLANK + DELIMITER + BLANK, price),
            
            new KeyValue<>(timeBucket + DELIMITER + action + DELIMITER + BLANK + DELIMITER + BLANK + DELIMITER + BLANK, price)
        );

        return records;
    }
    
    private String generate1MinuteBucket(Instant timestamp) {
        // Truncate to the nearest minute and format it
        Instant truncatedTime = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return BUCKET_FORMATTER.format(truncatedTime);
    }
}
