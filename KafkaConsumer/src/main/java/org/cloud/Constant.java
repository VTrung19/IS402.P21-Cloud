package org.cloud;

public class Constant {

    public static final String KAFKA_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss";
    public static final String DATE_STRING_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final class InvoiceItemFields {
        public static final String TRANSACTION_ID = "TransactionID";
        public static final String CUSTOMER_ID = "CustomerID";
        public static final String TRANSACTION_TIME = "TransactionTime";
        public static final String STORE = "Store";
        public static final String PRODUCT_ID = "ProductID";
        public static final String TOTAL_PRICE = "TotalPrice";
        public static final String QUANTITY = "Quantity";
    }

    public static final class KafkaSourceConfig {
        public static final String TOPIC_NAME = ConfigLoader.get("kafka.topic");
        public static final String BOOTSTRAP_SERVERS = ConfigLoader.get("kafka.bootstrap.servers");
        public static final String GROUP_ID = ConfigLoader.get("kafka.group.id");
        public static final String AUTO_COMMIT = ConfigLoader.get("enable.auto.commit");
        public static final String OFFSET_RESET = ConfigLoader.get("auto.offset.reset");

        public static final String SOURCE_NAME= "Kafka Source";
    }

    public static final class PostgresqlConfig {
        public static final String DRIVER_NAME = ConfigLoader.get("org.postgresql.Driver");

        public static final String JDBC_URL = ConfigLoader.get("jdbc.url");
        public static final String USERNAME = ConfigLoader.get("jdbc.username");
        public static final String PASSWORD = ConfigLoader.get("jdbc.password");
    }
}
