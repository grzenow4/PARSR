package allezon.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import allezon.domain.AggregatedValue;
import allezon.domain.UserTagEvent;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random; 

@Configuration
public class KafkaStreamsConfig {
    private final static String APP_ID = "allezon-analytics-app";
    public final static String STATE_STORE_NAME_KEY_VALUE_NAME = "allezon-s-s-1";
    private final static String STATE_STORE_OUTPUT_TOPIC = "allezon-aggregated-actions-input";
    public final static String ANALYTICS_INPUT_TOPIC = "analytics-input";
    private final static String BLANK = "*";
    public static final DateTimeFormatter BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]").withZone(ZoneOffset.UTC);


    @Bean
    public Properties kafkaStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "st124vm101.rtb-lab.pl:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        return props;
    }

    @Bean
    public KafkaStreams kafkaStreams(
            StreamsBuilder builder,
            Properties kafkaStreamsProperties) {
    
        JsonSerde<UserTagEvent> userTagEventSerde = new JsonSerde<>(UserTagEvent.class);
        JsonSerde<AggregatedValue> aggregatedValueSerde = new JsonSerde<>(AggregatedValue.class);
    
        KStream<String, UserTagEvent> userTagEventsStream = builder.stream(
                ANALYTICS_INPUT_TOPIC,
                Consumed.with(Serdes.String(), userTagEventSerde)
        );
    
        KStream<String, AggregatedValue> aggregatedStream = userTagEventsStream
                .flatMap((key, userTagEvent) -> reKeyInputStream(userTagEvent))
                .groupByKey(
                    Grouped.with(Serdes.String(), Serdes.Integer())
                )
                .aggregate(
                        AggregatedValue::new,
                        (key, value, aggregate) -> aggregate.aggregateProduct(value),
                        Materialized.<String, AggregatedValue, KeyValueStore<Bytes, byte[]>>as(STATE_STORE_NAME_KEY_VALUE_NAME)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggregatedValueSerde)
                )
                .toStream();
    
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
            new KeyValue<>(timeBucket + ":" + action + ":" + origin + ":" + brandId + ":" + categoryId, price),

            new KeyValue<>(timeBucket + ":" + action + ":" + BLANK + ":" + brandId + ":" + categoryId, price),
            new KeyValue<>(timeBucket + ":" + action + ":" + origin + ":" + BLANK + ":" + categoryId, price),
            new KeyValue<>(timeBucket + ":" + action + ":" + origin + ":" + brandId + ":" + BLANK, price),

            new KeyValue<>(timeBucket + ":" + action + ":" + BLANK + ":" + BLANK + ":" + categoryId, price),
            new KeyValue<>(timeBucket + ":" + action + ":" + BLANK + ":" + brandId + ":" + BLANK, price),
            new KeyValue<>(timeBucket + ":" + action + ":" + origin + ":" + BLANK + ":" + BLANK, price),
            
            new KeyValue<>(timeBucket + ":" + action + ":" + BLANK + ":" + BLANK + ":" + BLANK, price)
        );

        return records;
    }
    
    private String generate1MinuteBucket(Instant timestamp) {
        // Truncate to the nearest minute and format it
        Instant truncatedTime = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return BUCKET_FORMATTER.format(truncatedTime);
    }
}
