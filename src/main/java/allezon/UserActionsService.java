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

    private AerospikeClient client;
    Map<String, Map<Action, LinkedList<UserTagEvent>>> events = new ConcurrentHashMap<>();

    public UserActionsService(@Value("${aerospike.seeds}") String[] aerospikeSeeds, @Value("${aerospike.port}") int port) {
        this.client = new AerospikeClient(defaultClientPolicy(), Arrays.stream(aerospikeSeeds).map(seed -> new Host(seed, port)).toArray(Host[]::new));

        // String indexesInfo = Info.request(client.getNodes()[0], "sindex/" + NAMESPACE);
        // if (!indexesInfo.contains("cookie_index")) {
        //     IndexTask cookieIndexTask = client.createIndex(null, NAMESPACE, SET, "cookie_index", "cookie", IndexType.STRING);
        //     cookieIndexTask.waitTillComplete();   
        // }
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
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        String serializedEvent = mapper.writeValueAsString(userTag);

        String keyString = userTag.getCookie() + ":" + userTag.getAction();
        Key key = new Key(NAMESPACE, SET, keyString);

        WritePolicy writePolicy = new WritePolicy();
        writePolicy.recordExistsAction = RecordExistsAction.UPDATE;

        while (true) {
            Record record = client.get(null, key);
            int generation = (record != null) ? record.generation : 0;

            // Append the new tag to the list
            Operation appendOperation = ListOperation.append("tags", com.aerospike.client.Value.get(serializedEvent));
            Operation sizeOperation = ListOperation.size("tags");

            writePolicy.generationPolicy = (record != null) ? GenerationPolicy.EXPECT_GEN_EQUAL : GenerationPolicy.NONE;
            writePolicy.generation = generation;

            try {
                Record newRecord = client.operate(writePolicy, key, appendOperation, sizeOperation);
                int currentSize = newRecord.getInt("tags_size");

                // Trim the list if it exceeds the maximum size
                if (currentSize > 2 * MAX_LIST_SIZE) {
                    Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
                    client.operate(writePolicy, key, trimOperation);
                    log.info("just trimmed the guy!!!");
                }
                break;
            } catch (AerospikeException ae) {
                if (ae.getResultCode() == ResultCode.GENERATION_ERROR) {
                    log.info("Generation error, retrying...");
                } else {
                    throw ae; // For other exceptions, rethrow
                }
            }
        }
    } catch (JsonProcessingException e) {
        log.error("Serialization failed", e);
    } catch (Exception e) {
        log.error("Failed to add UserTagEvent", e);
    }

    return ResponseEntity.noContent().build();
}
    // public ResponseEntity<Void> addUserTag(UserTagEvent userTag) {
    //     try {
    //         // log.info("try to add ??");
    //         // Serialize the UserTagEvent to JSON
    //         ObjectMapper mapper = new ObjectMapper();
    //         mapper.registerModule(new JavaTimeModule());
    //         String serializedEvent = mapper.writeValueAsString(userTag);

    //         // Create a unique key for the user tag list
    //         String keyString = userTag.getCookie() + ":" + userTag.getAction();
    //         Key key = new Key(NAMESPACE, SET, keyString);

    //         // Create the list operation to append the serialized event
    //         Operation appendOperation = ListOperation.append("tags", com.aerospike.client.Value.get(serializedEvent));

    //         // Create the list size operation to check the size after appending
    //         Operation sizeOperation = ListOperation.size("tags");

    //         // Perform the operations
    //         WritePolicy writePolicy = new WritePolicy();
    //         writePolicy.recordExistsAction = RecordExistsAction.UPDATE; // Update the list if it exists
    //         Record record = client.operate(writePolicy, key, appendOperation, sizeOperation);

    //         // Check the size of the list
    //         int currentSize = record.getInt("tags_size");

    //         // Trim the list if it exceeds the maximum size
    //         if (currentSize > 2 * MAX_LIST_SIZE) {
    //             Operation trimOperation = ListOperation.removeRange("tags", 0, currentSize - MAX_LIST_SIZE - 1);
    //             client.operate(writePolicy, key, trimOperation);
    //         }
    //         // log.info("added good");
    //     } catch (JsonProcessingException e) {
    //         log.error("failed1", e);
    //     } catch (Exception e) {
    //         log.error("failed2", e);
    //     }
    //     return ResponseEntity.noContent().build();
    // }

    private List<UserTagEvent> getUserTagsForAction(String cookie, Action action, TimeBound timeBound, int limit) {
        // log.info("looking for {} for cookie {}", action, cookie);
        List<UserTagEvent> userTags = new ArrayList<>();

        // Create a unique key for the user tag list
        String keyString = cookie + ":" + action;
        Key key = new Key(NAMESPACE, SET, keyString);

        // ReadPolicy to specify read behavior
        Policy readPolicy = new Policy(client.readPolicyDefault);
        readPolicy.socketTimeout = 1000;
        readPolicy.totalTimeout = 1000;

        try {
            // Fetch the list of tags from Aerospike
            Record record = client.get(readPolicy, key);
            if (record != null) {
                // Safely cast the result to a List of Strings
                List<?> rawList = record.getList("tags");
                if (rawList != null) {
                    List<String> serializedEvents = rawList.stream()
                                                          .map(Object::toString)
                                                          .collect(Collectors.toList());
                    ObjectMapper mapper = new ObjectMapper();
                    mapper.registerModule(new JavaTimeModule());
                    // Deserialize and filter the tags based on the time range
                    for (String serializedEvent : serializedEvents) {
                        UserTagEvent userTag = mapper.readValue(serializedEvent, UserTagEvent.class);

                        if (!userTag.getTime().isBefore(timeBound.getStartDate()) &&
                            userTag.getTime().isBefore(timeBound.getEndDate())) {
                            userTags.add(userTag);
                        }
                    }

                    // Sort and limit the results
                    userTags = userTags.stream()
                            .sorted((e1, e2) -> e2.getTime().compareTo(e1.getTime()))
                            .limit(limit)
                            .collect(Collectors.toList());
                }
            }
        } catch (Exception e) {
            log.error("Failed to retrieve UserTagEvents for action {}", action, e);
        }
        // log.info("for {} found {} events", keyString, userTags.size());
        return userTags;
    }
    
    public ResponseEntity<UserProfileResult> getUserProfile(String cookie,
                                                            String timeRangeStr,
                                                            int limit,
                                                            UserProfileResult expectedResult) {
        TimeBound timeBound;
        try {
            timeBound = getTimeBounds(timeRangeStr);
        } catch (Exception e) {
            log.error("Failed to parse time range", e);
            return ResponseEntity.badRequest().build();
        }
        List<UserTagEvent> views = getUserTagsForAction(cookie, Action.VIEW, timeBound, limit);
        List<UserTagEvent> buys = getUserTagsForAction(cookie, Action.BUY, timeBound, limit);
        UserProfileResult myResult = new UserProfileResult(cookie, views, buys);
        return ResponseEntity.ok(myResult);
    }

    private TimeBound getTimeBounds(String timeRangeStr) throws Exception {
        String[] timeRangeParts = timeRangeStr.split("_");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]");
        LocalDateTime fromTime = LocalDateTime.parse(timeRangeParts[0], formatter);
        LocalDateTime toTime = LocalDateTime.parse(timeRangeParts[1], formatter);
        return new TimeBound(fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    }

    private List<UserTagEvent> getEventsByAction(String cookie, int limit, TimeBound timeBound, Action action) {
        return events.get(cookie).getOrDefault(action, new LinkedList<>()).stream()
            .sorted(Comparator.comparing(UserTagEvent::getTime).reversed())
            .filter(e -> !e.getTime().isBefore(timeBound.getStartDate()) && e.getTime().isBefore(timeBound.getEndDate()))
            .limit(limit)
            .collect(Collectors.toList());
    }

    private void logDiscrepancies(UserProfileResult expectedResult, UserProfileResult myResult) {
        if (expectedResult != null) {
            if (myResult.getViews().size() != expectedResult.getViews().size()) {
                log.info("Wrong view count. Expected {}, got {}", expectedResult.getViews().size(), myResult.getViews().size());
            }
            if (myResult.getBuys().size() != expectedResult.getBuys().size()) {
                log.info("Wrong buy count. Expected {}, got {}", expectedResult.getBuys().size(), myResult.getBuys().size());
            }
        }
    }

}
