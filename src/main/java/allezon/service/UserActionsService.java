package allezon.service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.time.temporal.ChronoUnit;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import allezon.domain.Action;
import allezon.domain.Aggregate;
import allezon.domain.AggregatedValue;
import allezon.domain.AggregatesQueryResult;
import allezon.domain.TimeBound;
import allezon.domain.UserProfileResult;
import allezon.domain.UserTagEvent;

import static allezon.constant.Constants.*;

@Service
public class UserActionsService {
    private static final Logger log = LoggerFactory.getLogger(UserActionsService.class);
    private final Producer<String, UserTagEvent> kafkaProducer;
    private final ObjectMapper mapper;
    private final AerospikeService aerospikeService;

    @Autowired
    public UserActionsService(Producer<String, UserTagEvent> kafkaProducer,
                                ObjectMapper mapper,
                                AerospikeService aerospikeService) {
        this.kafkaProducer = kafkaProducer;
        this.mapper = mapper;
        this.aerospikeService = aerospikeService;
    }

    public ResponseEntity<Void> addUserTag(UserTagEvent userTag) {
        try {
            serializeAndSave(userTag);
        } catch (JsonProcessingException e) {
            log.error("Serialization failed", e);
        } catch (Exception e) {
            log.error("Failed to add UserTagEvent", e);
        }

        return ResponseEntity.noContent().build();
    }

    private void serializeAndSave(UserTagEvent userTag) throws JsonProcessingException {
        String serializedEvent = mapper.writeValueAsString(userTag);
        Key key = aerospikeService.createKey(userTag.getCookie(), userTag.getAction());
        boolean success = false;
        while (!success) {
            success = aerospikeService.saveUserTag(serializedEvent, key);
        }
        sendToAnalytics(userTag);
    }
    
    public ResponseEntity<UserProfileResult> getUserProfile(String cookie,
                                                            String timeRangeStr,
                                                            int limit,
                                                            UserProfileResult expectedResult) {
        TimeBound timeBound = TimeBound.fromTimeRangeStr(timeRangeStr);
        List<UserTagEvent> views = aerospikeService.getUserTagsForAction(cookie, Action.VIEW, timeBound, limit);
        List<UserTagEvent> buys = aerospikeService.getUserTagsForAction(cookie, Action.BUY, timeBound, limit);
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
        List<String> timeBuckets = getTimeBuckets(timeRangeStr);
        List<String> columns = createColumnNames(origin, brandId, categoryId, aggregates);
        List<List<String>> rows = createAllRows(timeBuckets, action, aggregates, origin, brandId, categoryId);
        AggregatesQueryResult result = new AggregatesQueryResult(columns, rows);      
        if (!result.equals(expectedResult)) {
            log.info("produced:");
            log.info(result.toString());
            log.info("expected:");
            log.info(expectedResult.toString());
            log.info("_______________");
        } 
        return ResponseEntity.ok(result);                                                        
    }

    private List<List<String>> createAllRows(List<String> timeBuckets, Action action, List<Aggregate> aggregates,
                                                String origin, String brandId, String categoryId) {
        List<List<String>> rows = new LinkedList<>();
        for (String bucket : timeBuckets) {
            List<String> row = createRow(bucket, action, aggregates, origin, brandId, categoryId);
            rows.add(row);
        }        
        return rows;
    }

    private List<String> createRow(String bucket, Action action, List<Aggregate> aggregates, String origin, String brandId, String categoryId) {
        String key = makeKeyWithBlanks(bucket, action.toString(), origin, brandId, categoryId);
        Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key);
        Record record = aerospikeService.get(null, aerospikeKey);
        if (record == null) {
            log.info("record is null, key: {}", key);
        }
        AggregatedValue value = new AggregatedValue(record.getLong("count"), record.getLong("price"));
        return formatRow(bucket, action.toString(), aggregates, origin, brandId, categoryId, value);
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

    private List<String> formatRow(String timeBucket,
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

    private void sendToAnalytics(UserTagEvent userTag) {
        String kafkaKey = generate1MinuteBucket(userTag.getTime()) + ":" + userTag.getAction().toString();
        kafkaProducer.send(new ProducerRecord<String, UserTagEvent>(ANALYTICS_INPUT_TOPIC, kafkaKey, userTag));    
    }

    private String generate1MinuteBucket(Instant timestamp) {
        Instant truncatedTime = timestamp.truncatedTo(ChronoUnit.MINUTES);
        return BUCKET_FORMATTER.format(truncatedTime);
    }

    private List<String> getTimeBuckets(String timeRangeStr) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
        String[] times = timeRangeStr.split("_");
        LocalDateTime timeFrom = LocalDateTime.parse(times[0], formatter);
        LocalDateTime timeTo = LocalDateTime.parse(times[1], formatter);

        List<String> result = new ArrayList<>();

        while (timeFrom.isBefore(timeTo)) {
            result.add(timeFrom.format(formatter));
            timeFrom = timeFrom.plusMinutes(1);
        }

        return result;    
    }
}
