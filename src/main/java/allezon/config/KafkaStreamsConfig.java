package allezon.config;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

import allezon.domain.AggregatedValue;
import allezon.domain.UserTagEvent;
import allezon.service.UserActionsService;

import static allezon.constant.Constants.COUNT_BIN;
import static allezon.constant.Constants.NAMESPACE;
import static allezon.constant.Constants.PRICE_BIN;
import static allezon.constant.Constants.SET_ANALYTICS;
import static allezon.constant.Constants.BLANK;
import static allezon.constant.Constants.DELIMITER;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;

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
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsConfig.class);
    private final static String APP_ID = "allezonapp";
    public final static String STATE_STORE_NAME_KEY_VALUE_NAME = "allezonson";
    public final static String ANALYTICS_INPUT_TOPIC = "analytical";                     
    public static final DateTimeFormatter BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss").withZone(ZoneOffset.UTC);


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
            .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofSeconds(60), Duration.ofSeconds(0)))
            .aggregate(
                AggregatedValue::new,
                (key, event, aggregate) -> aggregate.aggregateProduct(event),
                Materialized.with(Serdes.String(), aggregatedValueSerde) 
        );

        aggregatedTable.toStream().foreach((key, value) -> {
            while (true) {
                try {
                    Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key.key());
    
                    // Fetch existing record if available
                    Record existingRecord = client.get(null, aerospikeKey);
                
                    // Initialize existing values as 0 if the record doesn't exist
                    int existingCount = 0;
                    int existingPrice = 0;
                
                    if (existingRecord != null) {
                        existingCount = existingRecord.getInt(COUNT_BIN); // Get the existing count
                        existingPrice = existingRecord.getInt(PRICE_BIN); // Get the existing price
                    }
                
                    // Add the new values to the existing ones
                    int newCount = existingCount + value.getCount();
                    int newPrice = existingPrice + value.getPrice();
                
                    // Create new bins with the accumulated values
                    Bin countBin = new Bin(COUNT_BIN, newCount);
                    Bin priceBin = new Bin(PRICE_BIN, newPrice);
                
                    WritePolicy writePolicy = new WritePolicy();
                    writePolicy.recordExistsAction = RecordExistsAction.UPDATE;  
                    writePolicy.generationPolicy = (existingRecord != null) ? GenerationPolicy.EXPECT_GEN_EQUAL : GenerationPolicy.NONE;
                    writePolicy.generation = (existingRecord != null) ? existingRecord.generation : 0;
                    writePolicy.expiration = 24 * 60 * 60;
                
                    // Put the accumulated values back into Aerospike
                    client.put(writePolicy, aerospikeKey, countBin, priceBin);
                    break;
                } catch (AerospikeException ae) {
                    if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
                        log.info("Generation error, retrying...");
                        continue;
                    } else {
                        log.error("Error in addEvent", ae);
                        throw ae; // For other exceptions, rethrow
                    }
                } catch (Exception e) {
                    log.error("Some other exception", e);
                    throw e;
                }
            }
            // Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key.key());
            // Bin countBin = new Bin(COUNT_BIN, value.getCount());
            // Bin priceBin = new Bin(PRICE_BIN, value.getPrice());
            // WritePolicy writePolicy = new WritePolicy();
            // // writePolicy.expiration = 24 * 60 * 60;
            // client.put(writePolicy, aerospikeKey, countBin, priceBin);
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
