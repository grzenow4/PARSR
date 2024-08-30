package allezon;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.GenerationPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.Replica;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.RecordSet;
import com.aerospike.client.query.Statement;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import allezon.domain.Action;
import allezon.domain.TimeBound;
import allezon.domain.UserProfileResult;
import allezon.domain.UserTagEvent;

@Service
public class UserActionsService {
    private static final String NAMESPACE = "parsr";
    private static final String SET = "user_tags";
    private static final int MAX_LIST_SIZE = 200;

    private static final Logger log = LoggerFactory.getLogger(UserActionsResource.class);

    private ObjectMapper mapper;
    private AerospikeClient client;

    public UserActionsService(@Value("${aerospike.seeds}") String[] aerospikeSeeds, @Value("${aerospike.port}") int port) {
        this.client = new AerospikeClient(defaultClientPolicy(), Arrays.stream(aerospikeSeeds).map(seed -> new Host(seed, port)).toArray(Host[]::new));
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    private static ClientPolicy defaultClientPolicy() {
        ClientPolicy defaultClientPolicy = new ClientPolicy();
        defaultClientPolicy.readPolicyDefault.replica = Replica.MASTER_PROLES;
        defaultClientPolicy.readPolicyDefault.socketTimeout = 1000;
        defaultClientPolicy.readPolicyDefault.totalTimeout = 1000;
        defaultClientPolicy.writePolicyDefault.socketTimeout = 15000;
        defaultClientPolicy.writePolicyDefault.totalTimeout = 15000;
        defaultClientPolicy.writePolicyDefault.maxRetries = 1;
        defaultClientPolicy.writePolicyDefault.commitLevel = CommitLevel.COMMIT_MASTER;
        defaultClientPolicy.writePolicyDefault.recordExistsAction = RecordExistsAction.REPLACE;
        return defaultClientPolicy;
    }

    public ResponseEntity<Void> addUserTag(UserTagEvent userTag) {
        try {
            String serializedEvent = mapper.writeValueAsString(userTag);
            Key key = createKey(userTag.getCookie(), userTag.getAction());
            boolean success = false;
            while (!success) {
                success = saveUserTag(serializedEvent, key);
            }
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

    private boolean saveUserTag(String serializedEvent, Key key) {
        Record record = client.get(null, key);
        Operation appendOperation = ListOperation.append("tags", com.aerospike.client.Value.get(serializedEvent));
        Operation sizeOperation = ListOperation.size("tags");

        WritePolicy writePolicy = createWritePolicy(record);
        try {
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
                throw ae; // For other exceptions, rethrow
            }
        }
    }

    private Key createKey(String cookie, Action action) {
        String keyString = cookie + ":" + action;
        return new Key(NAMESPACE, SET, keyString);
    }

    private boolean isListTooLarge(int currentSize) {
        return currentSize > 2 * MAX_LIST_SIZE;
    }

    private void removeOutdatedRecords(Record newRecord, int currentSize, Key key) {
        Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
        WritePolicy writePolicy = createWritePolicy(newRecord);
        client.operate(writePolicy, key, trimOperation);
        log.info("just trimmed the list - it had {} records", currentSize);
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
}
