package allezon.constant;

import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class Constants {
    public final static String APP_ID = "app-allezon";
    public final static String STATE_STORE_NAME_KEY_VALUE_NAME = "statestore-allezon";
    public final static String ANALYTICS_INPUT_TOPIC = "all.in";  

    public static final String BUCKET_COLUMN_NAME = "1m_bucket";
    public static final String ACTION_COLUMN_NAME = "action";
    public static final String ORIGIN_COLUMN_NAME = "origin";
    public static final String BRAND_COLUMN_NAME = "brand_id";
    public static final String CATEGORY_COLUMN_NAME = "category_id";
    public static final String COUNT_COLUMN_NAME = "count";
    public static final String SUM_PRICE_COLUMN_NAME = "sum_price";

    public static final String COUNT_BIN = "count";
    public static final String PRICE_BIN = "price";

    public static final String NAMESPACE = "parsr";
    public static final String SET_USER_TAGS = "user_tags";
    public static final String SET_ANALYTICS = "analytics";
    public static final int MAX_LIST_SIZE = 200;
    public static final DateTimeFormatter BUCKET_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS]").withZone(ZoneOffset.UTC);

    public static final int AGGREGATION_WINDOW_SIZE = 20;

    public final static String BLANK = "*";
    public final static String DELIMITER = ":";
}
