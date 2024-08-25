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
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Info;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.CommitLevel;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.Replica;
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

    private static final Logger log = LoggerFactory.getLogger(UserActionsResource.class);

    private AerospikeClient client;
    Map<String, Map<Action, LinkedList<UserTagEvent>>> events = new ConcurrentHashMap<>();

    public UserActionsService(@Value("${aerospike.seeds}") String[] aerospikeSeeds, @Value("${aerospike.port}") int port) {
        this.client = new AerospikeClient(defaultClientPolicy(), Arrays.stream(aerospikeSeeds).map(seed -> new Host(seed, port)).toArray(Host[]::new));

        String indexesInfo = Info.request(client.getNodes()[0], "sindex/" + NAMESPACE);
        if (!indexesInfo.contains("cookie_index")) {
            IndexTask cookieIndexTask = client.createIndex(null, NAMESPACE, SET, "cookie_index", "cookie", IndexType.STRING);
            cookieIndexTask.waitTillComplete();   
        }
        if (!indexesInfo.contains("action_index")) {
            IndexTask actionIndexTask = client.createIndex(null, NAMESPACE, SET, "action_index", "action", IndexType.STRING);
            actionIndexTask.waitTillComplete();   
        }
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
            // log.info("try to add things");
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            String serializedEvent = mapper.writeValueAsString(userTag);
            
            String keyString = userTag.getCookie() + ":" + userTag.getAction() + ":" + userTag.getTime();
            Key key = new Key(NAMESPACE, SET, keyString);
            
            Bin eventBin = new Bin("event", serializedEvent);
            Bin cookieBin = new Bin("cookie", userTag.getCookie());
            
            client.put(null, key, eventBin, cookieBin);
            
            // log.info("UserTagEvent added successfully with key: {}", keyString);
        } catch (Exception e) {
            e.printStackTrace();
            log.info("Failed to add UserTagEvent to Aerospike.");
        }
        return ResponseEntity.noContent().build();
    }

    public List<UserTagEvent> getUserTags(String cookie, Action action, TimeBound timeBound) {
        List<UserTagEvent> userTags = new ArrayList<>();
    
        try {
            // log.info("try to get some things");
            Statement statement = new Statement();
            statement.setNamespace(NAMESPACE);
            statement.setSetName(SET);
            
            // Applying filters based on cookie
            statement.setFilter(Filter.equal("cookie", cookie));
    
            RecordSet rs = client.query(null, statement);
    
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());

            while (rs.next() && userTags.size() < 200) {
                Record record = rs.getRecord();
                String serializedEvent = record.getString("event");
                UserTagEvent userTag = mapper.readValue(serializedEvent, UserTagEvent.class);
                // log.info("for cookie: {}, event is: {}", cookie, userTag.toString());
                if (userTag.getAction().equals(action) && 
                    !userTag.getTime().isBefore(timeBound.getStartDate()) &&
                    userTag.getTime().isBefore(timeBound.getEndDate())) {
                    userTags.add(userTag);                    
                }
            }
    
            rs.close();
            // log.info("Retrieved {} user tagsfor cookie {} and action {}", userTags.size(), cookie, action);
        } catch (Exception e) {
            e.printStackTrace();
            log.info("Failed to retrieve UserTagEvents.");
        }
        userTags.sort((e1, e2) -> e2.getTime().compareTo(e1.getTime()));
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

        List<UserTagEvent> b = getUserTags(cookie, Action.BUY, timeBound);
        List<UserTagEvent> v = getUserTags(cookie, Action.VIEW, timeBound);
        if (expectedResult.getBuys().size() != b.size()) {
            log.info("Wrong number of buys. Expected {} but found {}", expectedResult.getBuys().size(), b.size());
        }
        if (expectedResult.getViews().size() != v.size()) {
            log.info("Wrong number of views. Expected {} but found {}", expectedResult.getViews().size(), v.size());
        }
        UserProfileResult myResult = new UserProfileResult(cookie, v, b);
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
