package allezon.domain;

import java.time.Instant;

import lombok.Data;

@Data
public class TimeBound {
    private Instant startDate;
    private Instant endDate;
    
    public TimeBound(Instant startDate, Instant endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }
}
