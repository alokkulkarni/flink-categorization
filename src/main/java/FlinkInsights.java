import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
import java.util.Properties;

public class FlinkInsights {
    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.getConfig().setLocalTimeZone(ZoneId.of("UTC"));


        String topic = "transactions";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "flink-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");



        FlinkKafkaConsumer<Transactions> consumer = new FlinkKafkaConsumer<>(topic, new JsonValueDeserializer(), properties);
        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);

        DataStream<Transactions> transactionStream = env.addSource(consumer).name("Kafka Source");
        transactionStream.print();

        JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
                .withBatchSize(5000)
                .withBatchIntervalMs(200)
                .withMaxRetries(5)
                .build();

        JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build();

        Table transactions = tableEnv.fromDataStream(transactionStream);
        transactions.printSchema();

        tableEnv.createTemporaryView("transactions", transactions);

        Table savingsResult = tableEnv.sqlQuery(
                "SELECT customerId, " +
                        "accountNumber, " +
                        "EXTRACT(YEAR FROM transaction_time), " +
                        "SUM(CASE " +
                            "WHEN transaction_type = 'credit' THEN transaction_amount " +
                            "ELSE -transaction_amount " +
                        "END) as total_amount " +
                        "FROM transactions " +
                        "WHERE EXTRACT(MONTH FROM transaction_time) <= (EXTRACT(MONTH FROM CURRENT_DATE)) AND EXTRACT(MONTH FROM transaction_time) >= (EXTRACT(MONTH FROM CURRENT_DATE) - 2)" +
                        "AND EXTRACT(YEAR FROM transaction_time)=2024 " +
                        "GROUP BY customerId, accountNumber, EXTRACT(YEAR FROM transaction_time) "
        );

        savingsResult.execute().print();

        env.execute("Flink Insights");
    }
}
