package org.cloud;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.cloud.model.InvoiceItem;
import org.cloud.source.JdbcPostgresSink;
import org.cloud.source.KafkaInvoiceSource;
import org.cloud.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerApplication {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerApplication.class);

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2); // Tùy chỉnh số lượng task chạy song song
        KafkaSource<String> kafkaSource = KafkaInvoiceSource.createKafkaConsumer();

        DataStream<InvoiceItem> invoiceStream = env
                .fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), Constant.KafkaSourceConfig.SOURCE_NAME)
                .flatMap((String json, Collector<InvoiceItem> out) -> {
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        InvoiceItem item = mapper.readValue(json, InvoiceItem.class);
                        out.collect(item);
                    } catch (Exception e) {
                        log.error("JSON lỗi: {}", json, e);
                    }
                })
                .returns(InvoiceItem.class);

        // 1. Tổng doanh thu toàn hệ thống (Total Revenue)
        DataStream<Double> totalRevenueStream = invoiceStream
                .map(item -> item.totalPrice)
                .returns(Types.DOUBLE)
                .keyBy(x -> 0)
                .reduce(Double::sum);

        totalRevenueStream
                .map(total -> Utils.logWithTime("Tổng doanh thu: " + total))
                .print();

        totalRevenueStream.addSink(JdbcPostgresSink.createJdbcSink(
                "INSERT INTO total_revenue (total_revenue) VALUES (?)",
                (ps, total) -> {
                    ps.setDouble(1, total);
                }
        )).name("Total Revenue");

        // 2. Tổng số lượng sản phẩm bán ra (Total Quantity Sold)
        DataStream<Integer> totalQuantityStream = invoiceStream
                .map(item -> item.quantity)
                .returns(Types.INT)
                .keyBy(x -> 0)
                .reduce(Integer::sum);

        totalQuantityStream
                .map(totalQty -> Utils.logWithTime("Tổng số lượng sản phẩm bán ra: " + totalQty))
                .print();

        totalQuantityStream.addSink(JdbcPostgresSink.createJdbcSink(
                "INSERT INTO total_quantity (total_quantity) VALUES (?)",
                (ps, totalQty) -> {
                    ps.setInt(1, totalQty);
                }
        )).name("Total Quantity");

        // 3. Doanh thu theo từng sản phẩm (Revenue by Product)
        DataStream<Tuple2<String, Double>> productRevenueStream = invoiceStream
                .map(item -> Tuple2.of(item.productId, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(tuple -> tuple.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

        productRevenueStream
                .map(t -> Utils.logWithTime("Doanh thu theo sản phẩm [" + t.f0 + "]: " + t.f1))
                .print();

        productRevenueStream.addSink(JdbcPostgresSink.createJdbcSink(
                "INSERT INTO product_revenue (product_id, total_revenue) VALUES (?, ?)",
                (ps, tuple) -> {
                    ps.setString(1, tuple.f0); // product_id
                    ps.setDouble(2, tuple.f1); // total_revenue
                }
        )).name("Product Revenue");

        // 4. Doanh thu theo từng cửa hàng (Revenue by Store)
        DataStream<Tuple2<String, Double>> storeRevenueStream = invoiceStream
                .map(item -> Tuple2.of(item.store, item.totalPrice))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE))
                .keyBy(t -> t.f0)
                .reduce((t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1));

        storeRevenueStream
                .map(t -> Utils.logWithTime("Doanh thu theo cửa hàng [" + t.f0 + "]: " + t.f1))
                .print();

        storeRevenueStream.addSink(JdbcPostgresSink.createJdbcSink(
                "INSERT INTO store_revenue (store_name, total_revenue) VALUES (?, ?)",
                (ps, tuple) -> {
                    ps.setString(1, tuple.f0); // store_name
                    ps.setDouble(2, tuple.f1); // total_revenue
                }
        )).name("Store Revenue");

        env.execute("Flink Invoice Processor");
    }
}
