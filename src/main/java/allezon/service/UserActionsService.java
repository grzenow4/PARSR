package allezon.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import allezon.domain.Action;
import allezon.domain.Aggregate;
import allezon.domain.AggregatedValue;
import allezon.domain.AggregatesQueryResult;
import allezon.domain.TimeBound;
import allezon.domain.UserProfileResult;
import allezon.domain.UserTagEvent;
import allezon.config.KafkaStreamsConfig;

import static allezon.constant.Constants.*;
import static allezon.config.KafkaProducerConfig.*;

@Service
public class UserActionsService {
    private static final Logger log = LoggerFactory.getLogger(UserActionsService.class);
    private final Producer<String, UserTagEvent> kafkaProducer;
    private final ObjectMapper mapper;
    private final AerospikeClient client;

    @Autowired
    public UserActionsService(AerospikeClient client,
                                Producer<String, UserTagEvent> kafkaProducer,
                                ObjectMapper mapper) {
        this.client = client;
        this.kafkaProducer = kafkaProducer;
        this.mapper = mapper;
    }

    public ResponseEntity<Void> addUserTag(UserTagEvent userTag) {
        try {
            String serializedEvent = mapper.writeValueAsString(userTag);
            Key key = createKey(userTag.getCookie(), userTag.getAction());
            boolean success = false;
            while (!success) {
                success = saveUserTag(serializedEvent, key);
            }
            sendToAnalytics(userTag);
        } catch (JsonProcessingException e) {
            log.error("Serialization failed", e);
        } catch (Exception e) {
            log.error("Failed to add UserTagEvent", e);
        }

        return ResponseEntity.noContent().build();
    }

    public ResponseEntity<UserProfileResult> getUserProfile(String cookie,
                                                            String timeRangeStr,
                                                            int limit,
                                                            UserProfileResult expectedResult) {
        TimeBound timeBound = getTimeBounds(timeRangeStr);
        List<UserTagEvent> views = getUserTagsForAction(cookie, Action.VIEW, timeBound, limit);
        List<UserTagEvent> buys = getUserTagsForAction(cookie, Action.BUY, timeBound, limit);
        UserProfileResult myResult = new UserProfileResult(cookie, views, buys);
        return ResponseEntity.ok(myResult);
    }

    public ResponseEntity<AggregatesQueryResult> getAggregates(String timeRangeStr,
                                                               Action action,
                                                               List<Aggregate> aggregates,
                                                               String origin,
                                                               String brandId,
                                                               String categoryId,
                                                               AggregatesQueryResult expectedResult) {
        log.info("got request for aggregates!");       
        List<String> timeBuckets = getTimeBuckets(timeRangeStr);
        List<String> columns = createColumnNames(origin, brandId, categoryId, aggregates);
        List<List<String>> rows = new LinkedList<>();
        for (String bucket : timeBuckets) {
            String key = makeKeyWithBlanks(bucket, action.toString(), origin, brandId, categoryId);
            log.info("asking for value for key {}", key);
            Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key);
            Record record = client.get(null, aerospikeKey);
            AggregatedValue value = new AggregatedValue(record.getInt("count"), record.getInt("price"));
            log.info("I actually got something! {}:{}", value.getCount(), value.getPrice());
            rows.add(createRow(bucket, action.toString(), aggregates, origin, brandId, categoryId, value));
        }          
        AggregatesQueryResult result = new AggregatesQueryResult(columns, rows);       
        log.info("actually got some values....");
        log.info("{}", result.toString());    
        log.info("while expected was");
        log.info("{}", expectedResult.toString());
        log.info("_________________________________"); 
        return ResponseEntity.ok(result);                                                        
    }

    private String makeKeyWithBlanks(String bucket, String action, String origin, String brandId, String categoryId) {
        StringBuilder sb = new StringBuilder();
        
        sb.append(bucket);
        sb.append(DELIMITER);

        sb.append(action);
        sb.append(DELIMITER);

        sb.append(fieldOrBlank(origin));
        sb.append(DELIMITER);
        
        sb.append(fieldOrBlank(brandId));
        sb.append(DELIMITER);

        sb.append(fieldOrBlank(categoryId));
        
        return sb.toString();
    }

    private String fieldOrBlank(String field) {
        return field != null ? field : BLANK;
    }


    private List<String> createColumnNames(String origin, String brandId, String categoryId, List<Aggregate> aggregates) {
        List<String> columns = new LinkedList<>();
        columns.add(BUCKET_COLUMN_NAME);
        columns.add(ACTION_COLUMN_NAME);
        if (origin != null) {
            columns.add(ORIGIN_COLUMN_NAME);
        }
        if (brandId != null) {
            columns.add(BRAND_COLUMN_NAME);
        }
        if (categoryId != null) {
            columns.add(CATEGORY_COLUMN_NAME);
        }
        for (Aggregate field : aggregates) {
            if (Aggregate.COUNT.equals(field)) {
                columns.add(COUNT_COLUMN_NAME);
            } else {
                columns.add(SUM_PRICE_COLUMN_NAME);
            }
        }
        return columns;
    }

    private List<String> createRow(String timeBucket,
                            String action,
                            List<Aggregate> aggregates,
                            String origin,
                            String brandId,
                            String categoryId,
                            AggregatedValue value) {
        List<String> row = new LinkedList<>();
        row.add(timeBucket);
        row.add(action);
        if (origin != null) {
            row.add(origin);
        }
        if (brandId != null) {
            row.add(brandId);
        }
        if (categoryId != null) {
            row.add(categoryId);
        }
        for (Aggregate field : aggregates) {
            if (Aggregate.COUNT.equals(field)) {
                row.add(String.valueOf(value.getCount()));
            } else {
                row.add(String.valueOf(value.getPrice()));
            }
        }
        return row;
    }


    private boolean saveUserTag(String serializedEvent, Key key) {
        try {
            Record record = client.get(null, key);
            Operation appendOperation = ListOperation.append("tags", com.aerospike.client.Value.get(serializedEvent));
            Operation sizeOperation = ListOperation.size("tags");
    
            WritePolicy writePolicy = createWritePolicy(record);
            Record newRecord = client.operate(writePolicy, key, appendOperation, sizeOperation);
            int currentSize = Integer.valueOf(newRecord.getList("tags").get(0).toString());

            if (isListTooLarge(currentSize)) {
                removeOutdatedRecords(newRecord, currentSize, key);
            }
            return true;
        } catch (AerospikeException ae) {
            if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
                log.info("Generation error, retrying...");
                return false;
            } else {
                log.error("Error in addEvent", ae);
                throw ae; // For other exceptions, rethrow
            }
        } catch (Exception e) {
            log.error("Some other exception", e);
            throw e;
        }
    }

    private void sendToAnalytics(UserTagEvent userTag) {
        String kafkaKey = generate1MinuteBucket(userTag.getTime()) + ":" + userTag.getAction().toString();
        kafkaProducer.send(new ProducerRecord<String, UserTagEvent>(KafkaStreamsConfig.ANALYTICS_INPUT_TOPIC, kafkaKey, userTag));    
    }

    private String generate1MinuteBucket(Instant timestamp) {
        // Truncate to the nearest minute and format it
        Instant truncatedTime = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return BUCKET_FORMATTER.format(truncatedTime);
    }

    private Key createKey(String cookie, Action action) {
        String keyString = cookie + ":" + action;
        return new Key(NAMESPACE, SET_USER_TAGS, keyString);
    }

    private boolean isListTooLarge(int currentSize) {
        return currentSize > 2 * MAX_LIST_SIZE;
    }

    private void removeOutdatedRecords(Record newRecord, int currentSize, Key key) {
        Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
        WritePolicy writePolicy = createWritePolicy(newRecord);
        client.operate(writePolicy, key, trimOperation);
    }

    private WritePolicy createWritePolicy(Record record) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;  
        writePolicy.generationPolicy = (record != null) ? GenerationPolicy.EXPECT_GEN_EQUAL : GenerationPolicy.NONE;
        writePolicy.generation = (record != null) ? record.generation : 0;
        return writePolicy;
    }

    private Policy createReadPolicy() {
        Policy readPolicy = new Policy(client.readPolicyDefault);
        readPolicy.socketTimeout = 1000;
        readPolicy.totalTimeout = 1000;
        return readPolicy;
    }

    private List<UserTagEvent> getUserTagsForAction(String cookie, Action action, TimeBound timeBound, int limit) {
        List<UserTagEvent> userTags = new ArrayList<>();
        Key key = createKey(cookie, action);
        Policy readPolicy = createReadPolicy();

        try {
            Record record = client.get(readPolicy, key);
            if (record != null) {
                List<?> rawList = record.getList("tags");
                if (rawList != null) {
                    List<String> serializedEvents = getSerializedEvents(rawList);
                    for (String serializedEvent : serializedEvents) {
                        appendUserTagList(serializedEvent, timeBound, userTags);
                    }
                    userTags = sortAndLimitUserTags(userTags, limit);
                }
            }
        } catch (Exception e) {
            log.error("Failed to retrieve UserTagEvents for action {}", action, e);
        }
        return userTags;
    }

    private List<String> getSerializedEvents(List<?> rawList) {
        return rawList.stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
    }

    private List<UserTagEvent> sortAndLimitUserTags(List<UserTagEvent> userTags, int limit) {
        return userTags.stream()
                .sorted((e1, e2) -> e2.getTime().compareTo(e1.getTime()))
                .limit(limit)
                .collect(Collectors.toList());
    }

    private void appendUserTagList(String serializedEvent, TimeBound timeBound, List<UserTagEvent> userTags) throws JsonMappingException, JsonProcessingException {
        UserTagEvent userTag = mapper.readValue(serializedEvent, UserTagEvent.class);

        if (!userTag.getTime().isBefore(timeBound.getStartDate()) &&
            userTag.getTime().isBefore(timeBound.getEndDate())) {
            userTags.add(userTag);
        }
    }

    private TimeBound getTimeBounds(String timeRangeStr) {
        String[] timeRangeParts = timeRangeStr.split("_");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]");
        LocalDateTime fromTime = LocalDateTime.parse(timeRangeParts[0], formatter);
        LocalDateTime toTime = LocalDateTime.parse(timeRangeParts[1], formatter);
        return new TimeBound(fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    }

    private List<String> getTimeBuckets(String timeRangeStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        // Split the time_range string into time_from and time_to
        String[] times = timeRangeStr.split("_");
        LocalDateTime timeFrom = LocalDateTime.parse(times[0], formatter);
        LocalDateTime timeTo = LocalDateTime.parse(times[1], formatter);

        List<String> result = new ArrayList<>();

        // Loop through each minute between timeFrom and timeTo (excluding timeTo)
        while (timeFrom.isBefore(timeTo)) {
            result.add(timeFrom.format(formatter));
            timeFrom = timeFrom.plusMinutes(1);
        }

        return result;    
    }

}
