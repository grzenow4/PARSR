package allezon.domain;

import lombok.Data;

@Data
public class AggregatedValue {
    private long count;
    private long price;

    public AggregatedValue() {
        
    }

    public AggregatedValue(long count, long price) {
        this.count = count;
        this.price = price;
    }

    public AggregatedValue aggregateProduct(long price) {
        this.count += 1;
        this.price += price;
        return this;
    }
}
