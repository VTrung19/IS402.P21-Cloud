package org.cloud;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.cloud.utils.SinkUtils;
import org.cloud.utils.TimeUtils;
import org.cloud.utils.Utils;

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
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .build();
    }

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = createKafkaConsumer();

        DataStream<String> jsonStream = env.fromSource(
                kafkaSource,
                org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks(),
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

        // 1. Tổng doanh thu toàn hệ thống (Total Revenue)
        DataStream<Double> totalRevenueStream = invoiceStream
                .map(item -> item.totalPrice)
                .returns(Types.DOUBLE)
                .keyBy(x -> 0)
                .reduce(Double::sum)
                .name("Total Revenue");

        totalRevenueStream
                .map(total -> Utils.logWithTime("Tổng doanh thu: " + total))
                .print();

        totalRevenueStream.addSink(SinkUtils.createJdbcSink(
                "INSERT INTO total_revenue (timestamp, total_revenue) VALUES (?, ?)",
                (ps, total) -> {
                    ps.setString(1, TimeUtils.getCurrentTime());
                    ps.setDouble(2, total);
                }
        ));

        // 2. Tổng số lượng sản phẩm bán ra (Total Quantity Sold)
        DataStream<Integer> totalQuantityStream = invoiceStream
                .map(item -> item.quantity)
                .returns(Types.INT)
                .keyBy(x -> 0)
                .reduce(Integer::sum)
                .name("Total Quantity");

        totalQuantityStream
                .map(totalQty -> Utils.logWithTime("Tổng số lượng sản phẩm bán ra: " + totalQty))
                .print();

        totalQuantityStream.addSink(SinkUtils.createJdbcSink(
                "INSERT INTO total_quantity (timestamp, total_quantity) VALUES (?, ?)",
                (ps, totalQty) -> {
                    ps.setString(1, TimeUtils.getCurrentTime());
                    ps.setInt(2, totalQty);
                }
        ));

        // 3. Doanh thu theo từng sản phẩm (Revenue by Product)
        DataStream<Tuple2<String, Double>> productRevenueStream = invoiceStream
                .map(item -> Tuple2.of(item.productId, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(tuple -> tuple.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .name("Product Revenue");

        productRevenueStream
                .map(t -> Utils.logWithTime("Doanh thu theo sản phẩm [" + t.f0 + "]: " + t.f1))
                .print();

        productRevenueStream.addSink(SinkUtils.createJdbcSink(
                "INSERT INTO product_revenue (product_id, total_revenue, timestamp) VALUES (?, ?, ?)",
                (ps, tuple) -> {
                    ps.setString(1, tuple.f0); // product_id
                    ps.setDouble(2, tuple.f1); // total_revenue
                    ps.setString(3, TimeUtils.getCurrentTime()); // timestamp
                }
        ));

        // 4. Doanh thu theo từng cửa hàng (Revenue by Store)
        DataStream<Tuple2<String, Double>> storeRevenueStream = invoiceStream
                .map(item -> Tuple2.of(item.store, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(t -> t.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .name("Store Revenue");

        storeRevenueStream
                .map(t -> Utils.logWithTime("Doanh thu theo cửa hàng [" + t.f0 + "]: " + t.f1))
                .print();

        storeRevenueStream.addSink(SinkUtils.createJdbcSink(
                "INSERT INTO store_revenue (store_name, total_revenue, timestamp) VALUES (?, ?, ?)",
                (ps, tuple) -> {
                    ps.setString(1, tuple.f0); // store_name
                    ps.setDouble(2, tuple.f1); // total_revenue
                    ps.setString(3, TimeUtils.getCurrentTime()); // timestamp
                }
        ));

        env.execute("Flink Invoice Processor");
    }
}
