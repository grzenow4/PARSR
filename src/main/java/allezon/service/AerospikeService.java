package allezon.service;

import static allezon.constant.Constants.COUNT_BIN;
import static allezon.constant.Constants.MAX_LIST_SIZE;
import static allezon.constant.Constants.NAMESPACE;
import static allezon.constant.Constants.PRICE_BIN;
import static allezon.constant.Constants.SET_ANALYTICS;
import static allezon.constant.Constants.SET_USER_TAGS;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.ListOperation;

import allezon.domain.Action;
import allezon.domain.AggregatedValue;
import allezon.domain.TimeBound;
import allezon.domain.UserTagEvent;

@Service
public class AerospikeService {
    private static final Logger log = LoggerFactory.getLogger(AerospikeService.class);
    private final AerospikeClient client;
    private final ObjectMapper mapper;

    @Autowired
    public AerospikeService(AerospikeClient client, ObjectMapper mapper) {
        this.client = client;
        this.mapper = mapper;
    }

    public Record get(Policy policy, Key key) {
        return client.get(policy, key);
    }

    public boolean writeAggregatesToAerospike(String key, AggregatedValue value) {
        try {
            Key aerospikeKey = new Key(NAMESPACE, SET_ANALYTICS, key);

            // Fetch existing record if available
            Record existingRecord = client.get(null, aerospikeKey);
        
            int existingCount = 0;
            int existingPrice = 0;
        
            if (existingRecord != null) {
                existingCount = existingRecord.getInt(COUNT_BIN);
                existingPrice = existingRecord.getInt(PRICE_BIN);
            }
        
            int newCount = existingCount + value.getCount();
            int newPrice = existingPrice + value.getPrice();
        
            Bin countBin = new Bin(COUNT_BIN, newCount);
            Bin priceBin = new Bin(PRICE_BIN, newPrice);
        
            WritePolicy writePolicy = createWritePolicy(existingRecord);
        
            client.put(writePolicy, aerospikeKey, countBin, priceBin);
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

    public boolean saveUserTag(String serializedEvent, Key key) {
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

    public List<UserTagEvent> getUserTagsForAction(String cookie, Action action, TimeBound timeBound, int limit) {
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

    private List<String> getSerializedEvents(List<?> rawList) {
        return rawList.stream()
                    .map(Object::toString)
                    .collect(Collectors.toList());
    }

    private boolean isListTooLarge(int currentSize) {
        return currentSize > 2 * MAX_LIST_SIZE;
    }

    private WritePolicy createWritePolicy(Record record) {
        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;  
        writePolicy.generationPolicy = (record != null) ? GenerationPolicy.EXPECT_GEN_EQUAL : GenerationPolicy.NONE;
        writePolicy.generation = (record != null) ? record.generation : 0;
        writePolicy.expiration = 24 * 60 * 60;
        return writePolicy;
    }

    private Policy createReadPolicy() {
        Policy readPolicy = new Policy(client.readPolicyDefault);
        readPolicy.socketTimeout = 1000;
        readPolicy.totalTimeout = 1000;
        return readPolicy;
    }

    private void removeOutdatedRecords(Record newRecord, int currentSize, Key key) {
        Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
        WritePolicy writePolicy = createWritePolicy(newRecord);
        client.operate(writePolicy, key, trimOperation);
    }

    public Key createKey(String cookie, Action action) {
        String keyString = cookie + ":" + action;
        return new Key(NAMESPACE, SET_USER_TAGS, keyString);
    }
}
