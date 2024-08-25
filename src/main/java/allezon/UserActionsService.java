package allezon;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import allezon.domain.Action;
import allezon.domain.TimeBound;
import allezon.domain.UserProfileResult;
import allezon.domain.UserTagEvent;

@Service
public class UserActionsService {
        private static final Logger log = LoggerFactory.getLogger(UserActionsResource.class);

    Map<String, Map<Action, LinkedList<UserTagEvent>>> events = new ConcurrentHashMap<>();

    public ResponseEntity<Void> addUserTag(UserTagEvent userTag) {
        if (!events.containsKey(userTag.getCookie())) {
            events.put(userTag.getCookie(), new ConcurrentHashMap<>());
            events.get(userTag.getCookie()).put(Action.BUY, new LinkedList<>());
            events.get(userTag.getCookie()).put(Action.VIEW, new LinkedList<>());
        }
        LinkedList<UserTagEvent> actionedEvent = events.get(userTag.getCookie()).get(userTag.getAction());
        synchronized (actionedEvent) {
            actionedEvent.add(userTag);
            if (actionedEvent.size() > 400) {
                actionedEvent = (LinkedList<UserTagEvent>) actionedEvent.subList(0, 200);
            }
            events.get(userTag.getCookie()).put(userTag.getAction(), actionedEvent);
        }
        return ResponseEntity.noContent().build();
    }

    public ResponseEntity<UserProfileResult> getUserProfile(String cookie,
                                                            String timeRangeStr,
                                                            int limit,
                                                            UserProfileResult expectedResult) {
        if (!events.containsKey(cookie)) {
            return ResponseEntity.ok(new UserProfileResult(cookie, new LinkedList<>(), new LinkedList<>()));
        }
        TimeBound timeBound;
        try {
            timeBound = getTimeBounds(timeRangeStr);
        } catch (Exception e) {
            log.error("Failed to parse time range", e);
            return ResponseEntity.badRequest().build();
        }

        List<UserTagEvent> views = getEventsByAction(cookie, limit, timeBound, Action.VIEW);
        List<UserTagEvent> buys = getEventsByAction(cookie, limit, timeBound, Action.BUY);
        UserProfileResult myResult = new UserProfileResult(cookie, views, buys);  

        logDiscrepancies(expectedResult, myResult);
        
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
