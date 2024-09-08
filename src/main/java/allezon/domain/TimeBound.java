package allezon.domain;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import lombok.Data;

@Data
public class TimeBound {
    private Instant startDate;
    private Instant endDate;
    
    public TimeBound(Instant startDate, Instant endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    public static TimeBound fromTimeRangeStr(String timeRangeStr) {
        String[] timeRangeParts = timeRangeStr.split("_");
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]");
        LocalDateTime fromTime = LocalDateTime.parse(timeRangeParts[0], formatter);
        LocalDateTime toTime = LocalDateTime.parse(timeRangeParts[1], formatter);
        return new TimeBound(fromTime.toInstant(ZoneOffset.UTC), toTime.toInstant(ZoneOffset.UTC));
    }

}
