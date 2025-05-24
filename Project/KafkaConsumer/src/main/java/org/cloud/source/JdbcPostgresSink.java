package org.cloud.source;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.cloud.Constant;

public class JdbcPostgresSink {
    public static JdbcConnectionOptions getJdbcConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(Constant.PostgresqlConfig.JDBC_URL)
                .withDriverName(Constant.PostgresqlConfig.DRIVER_NAME)
                .withUsername(Constant.PostgresqlConfig.USERNAME)
                .withPassword(Constant.PostgresqlConfig.PASSWORD).build();
    }

    public static <T> SinkFunction<T> createJdbcSink(String sql, JdbcStatementBuilder<T> statementBuilder) {
        return JdbcSink.sink(
                sql,
                statementBuilder,
                JdbcExecutionOptions.builder().withBatchSize(1).build(), // Lưu ngay lập tức dữ liệu mới mà không cần chờ batch size lớn
                getJdbcConnectionOptions()
        );
    }
}
