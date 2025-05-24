package org.cloud.source;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.cloud.Constant;

import java.util.Properties;

public class KafkaInvoiceSource {
    public static KafkaSource<String> createKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", Constant.KafkaSourceConfig.BOOTSTRAP_SERVERS);
        props.setProperty("group.id", Constant.KafkaSourceConfig.GROUP_ID);
        props.setProperty("auto.offset.reset", Constant.KafkaSourceConfig.OFFSET_RESET);
        props.setProperty("auto.offset.reset", Constant.KafkaSourceConfig.AUTO_COMMIT);

        return KafkaSource.<String>builder()
                .setProperties(props)
                .setTopics(Constant.KafkaSourceConfig.TOPIC_NAME)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();
    }
}
