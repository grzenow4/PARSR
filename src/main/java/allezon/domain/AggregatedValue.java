package allezon.domain;

import lombok.Data;

@Data
public class AggregatedValue {
    private int count;
    private int price;

    public AggregatedValue(int count, int price) {
        this.count = count;
        this.price = price;
    }

    public AggregatedValue aggregateProduct(int price) {
        this.count += 1;
        this.price += price;
        return this;
    }
}
