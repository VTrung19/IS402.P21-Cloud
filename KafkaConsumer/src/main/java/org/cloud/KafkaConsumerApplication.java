package org.cloud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaConsumerApplication {

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
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = createKafkaConsumer();

        DataStream<String> jsonStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                Constant.KafkaSourceConfig.SOURCE_NAME
        );

        DataStream<InvoiceItem> invoiceStream = jsonStream
                .flatMap((String json, Collector<InvoiceItem> out) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        InvoiceItem item = mapper.readValue(json, InvoiceItem.class);
                        out.collect(item);
                    } catch (Exception e) {
                        System.err.println("JSON lỗi: " + e.getMessage());
                    }
                })
                .returns(InvoiceItem.class);

        // 1. Tổng doanh thu toàn hệ thống
        invoiceStream
                .map(item -> item.totalPrice)
                .returns(Types.DOUBLE)
                .keyBy(x -> 0)
                .reduce(Double::sum)
                .map(total -> "[" + TimeUtils.getCurrentTime() + "]" + "Tổng doanh thu: " + total)
                .print();

        // 2. Tổng số lượng sản phẩm bán ra
        invoiceStream
                .map(item -> item.quantity)
                .returns(Types.INT)
                .keyBy(x -> 0)
                .reduce(Integer::sum)
                .map(totalQty -> "[" + TimeUtils.getCurrentTime() + "]" + "Tổng số lượng sản phẩm bán ra: " + totalQty)
                .print();

        // 3. Doanh thu theo từng sản phẩm
        invoiceStream
                .map(item -> Tuple2.of(item.productId, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(tuple -> tuple.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .map(t -> "[" + TimeUtils.getCurrentTime() + "]" + "Doanh thu theo sản phẩm [" + t.f0 + "]: " + t.f1)
                .print();

        // 4. Doanh thu theo từng cửa hàng
        invoiceStream
                .map(item -> Tuple2.of(item.store, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(t -> t.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .map(t -> "[" + TimeUtils.getCurrentTime() + "]" + "Doanh thu theo cửa hàng [" + t.f0 + "]: " + t.f1)
                .print();

        env.execute("Flink Invoice Processor");
    }
}
