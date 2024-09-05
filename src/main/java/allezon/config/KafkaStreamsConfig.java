package allezon.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.WritePolicy;

import allezon.domain.AggregatedValue;
import allezon.domain.UserTagEvent;

import static allezon.constant.Constants.COUNT_BIN;
import static allezon.constant.Constants.NAMESPACE;
import static allezon.constant.Constants.PRICE_BIN;
import static allezon.constant.Constants.SET_ANALYTICS;

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
    private final static String APP_ID = "allezonapp";
    public final static String STATE_STORE_NAME_KEY_VALUE_NAME = "allezonss";
    public final static String ANALYTICS_INPUT_TOPIC = "analyticsin";
    private final static String BLANK = "*";
    public static final DateTimeFormatter BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]").withZone(ZoneOffset.UTC);


    @Bean
    public Properties kafkaStreamsProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "st124vm101.rtb-lab.pl:9092,st124vm102.rtb-lab.pl:9092,st124vm103.rtb-lab.pl:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JsonSerde.class.getName());
        String stateDir = "/tmp/kafka-streams/" + System.getenv("HOSTNAME");
        System.out.println("set state dir to: " + stateDir);
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 10);  // Ensures matching partitions
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
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(10)))
            .aggregate(
                AggregatedValue::new,
                (key, event, aggregate) -> aggregate.aggregateProduct(event),
                Materialized.with(Serdes.String(), aggregatedValueSerde) 
        );

        aggregatedTable.toStream().foreach((key, value) -> {
            Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key.key());
            Bin countBin = new Bin(COUNT_BIN, value.getCount());
            Bin priceBin = new Bin(PRICE_BIN, value.getPrice());
            WritePolicy writePolicy = new WritePolicy();
            writePolicy.expiration = 24 * 60 * 60;
            client.put(writePolicy, aerospikeKey, countBin, priceBin);
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
