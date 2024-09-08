package allezon.service;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import allezon.domain.Action;
import allezon.domain.Aggregate;
import allezon.domain.AggregatesQueryResult;
import allezon.domain.UserProfileResult;
import allezon.domain.UserTagEvent;

@RestController
public class UserActionsResource {
    @Autowired
    UserActionsService userActionsService;

    @PostMapping("/user_tags")
    public ResponseEntity<Void> addUserTag(@RequestBody(required = false) UserTagEvent userTag) {
        return userActionsService.addUserTag(userTag);
    }

    @PostMapping("/user_profiles/{cookie}")
    public ResponseEntity<UserProfileResult> getUserProfile(@PathVariable("cookie") String cookie,
                                                            @RequestParam("time_range") String timeRangeStr,
                                                            @RequestParam(defaultValue = "200") int limit,
                                                            @RequestBody(required = false) UserProfileResult expectedResult) {
        return userActionsService.getUserProfile(cookie, timeRangeStr, limit, expectedResult);
    }

    @PostMapping("/aggregates")
    public ResponseEntity<AggregatesQueryResult> getAggregates(@RequestParam("time_range") String timeRangeStr,
            @RequestParam("action") Action action,
            @RequestParam("aggregates") List<Aggregate> aggregates,
            @RequestParam(value = "origin", required = false) String origin,
            @RequestParam(value = "brand_id", required = false) String brandId,
            @RequestParam(value = "category_id", required = false) String categoryId,
            @RequestBody(required = false) AggregatesQueryResult expectedResult) {
        return userActionsService.getAggregates(timeRangeStr, action, aggregates, origin, brandId, categoryId, expectedResult);
    }
}
