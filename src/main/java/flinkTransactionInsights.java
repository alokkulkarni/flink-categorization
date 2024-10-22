import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Properties;

@SuppressWarnings({"ExtractMethodRecommender", "DuplicatedCode"})
public class flinkTransactionInsights {

    private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
    private static final String username = "postgres";
    private static final String password = "postgres";

    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");

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


        //create transactions table
        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactions (" +
                        "transaction_id VARCHAR(255) PRIMARY KEY, " +
                        "transaction_time TIMESTAMP, " +
                        "transaction_category VARCHAR(255), " +
                        "transaction_description VARCHAR(255), " +
                        "transaction_amount DOUBLE PRECISION, " +
                        "transaction_type VARCHAR(255), " +
                        "transaction_currency VARCHAR(255), " +
                        "location VARCHAR(255), " +
                        "merchant VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "account_number VARCHAR(255) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS savingsPerAccountPerMonth (" +
                        "account_number VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "savings_amount DOUBLE PRECISION, " +
                        "amount_type VARCHAR(255), " +
                        "PRIMARY KEY (customer_id, account_number, year, month) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactionsPerCustomerPerCategoryPerMonth (" +
                        "account_number VARCHAR(255), " +
                        "customer_id VARCHAR(255), " +
                        "category VARCHAR(255), " +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "count INTEGER, " +
                        "amount DOUBLE PRECISION, " +
                        "amount_type VARCHAR(255), " +
                        "PRIMARY KEY (customer_id, account_number, category, year, month) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");

        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactionInsights (" +
                        "customer_id VARCHAR(255), " +
                        "account_number VARCHAR(255)," +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "transaction_type VARCHAR(255), " +
                        "transaction_amount DOUBLE PRECISION, " +
                        "total_transactions INT, " +
                        "total_savings DOUBLE PRECISION, " +
                        "total_savings_type VARCHAR(255), " +
                        "total_expenses DOUBLE PRECISION, " +
                        "total_income DOUBLE PRECISION, " +
                        "total_savings_percentage DOUBLE PRECISION, " +
                        "total_expenses_percentage DOUBLE PRECISION, " +
                        "PRIMARY KEY (customer_id, account_number, year, month) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Table Sink");


        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS transactionInsightsPerCustomerPerMerchantPerMonth (" +
                        "customer_id VARCHAR(255), " +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "transaction_type VARCHAR(255), " +
                        "transaction_amount DOUBLE PRECISION, " +
                        "transaction_category VARCHAR(255), " +
                        "total_transactions INT, " +
                        "merchant VARCHAR(255), " +
                        "total_spend_per_Merchant DOUBLE PRECISION, " +
                        "PRIMARY KEY (customer_id, year, month) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Transactions Per Mechant Spend Table Sink");


        transactionStream.addSink(JdbcSink.sink(
                "CREATE TABLE IF NOT EXISTS totalSpendAcrossMerchants (" +
                        "merchant VARCHAR(255), " +
                        "totalSpend DOUBLE PRECISION, " +
                        "count INT, " +
                        "year INTEGER, " +
                        "month INTEGER, " +
                        "PRIMARY KEY (merchant, year, month) " +
                        ")",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {

                },
                execOptions,
                connOptions
        )).name("Create Total Spend Across Merchants Table Sink");


//        ############################################################################################################
        // Insert into transactions table
//        ############################################################################################################
        transactionStream.addSink(JdbcSink.sink(
                "INSERT INTO transactions (transaction_id, transaction_time, transaction_category, " +
                        "transaction_description, transaction_amount, " +
                        "transaction_type, transaction_currency, location, merchant, customer_id, account_number) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                        "ON CONFLICT (transaction_id) DO UPDATE SET " +
                        "transaction_id = EXCLUDED.transaction_id, " +
                        "transaction_time = EXCLUDED.transaction_time, " +
                        "transaction_category = EXCLUDED.transaction_category, " +
                        "transaction_description = EXCLUDED.transaction_description, " +
                        "transaction_amount = EXCLUDED.transaction_amount, " +
                        "transaction_type = EXCLUDED.transaction_type, " +
                        "transaction_currency = EXCLUDED.transaction_currency, " +
                        "location = EXCLUDED.location, " +
                        "merchant = EXCLUDED.merchant, " +
                        "customer_id = EXCLUDED.customer_id, " +
                        "account_number = EXCLUDED.account_number",
                (JdbcStatementBuilder<Transactions>) (preparedStatement, transaction) -> {
                    preparedStatement.setString(1, transaction.getTransaction_id());
                    preparedStatement.setTimestamp(2, transaction.getTransaction_time());
                    preparedStatement.setString(3, transaction.getTransaction_category());
                    preparedStatement.setString(4, transaction.getTransaction_description());
                    preparedStatement.setDouble(5, transaction.getTransaction_amount());
                    preparedStatement.setString(6, transaction.getTransaction_type());
                    preparedStatement.setString(7, transaction.getTransaction_currency());
                    preparedStatement.setString(8, transaction.getLocation());
                    preparedStatement.setString(9, transaction.getMerchant());
                    preparedStatement.setString(10, transaction.getCustomerId());
                    preparedStatement.setString(11, transaction.getAccountNumber());
                },
                execOptions,
                connOptions
        )).name("Transactions Sink");

        transactionStream.map(transactions -> {
            int year = transactions.getTransaction_time().toLocalDateTime().getYear();
            int month = transactions.getTransaction_time().toLocalDateTime().getMonthValue();
            return new savingsPerAccountPerMonth(transactions.getCustomerId(), transactions.getAccountNumber(), year, month, transactions.getTransaction_amount(), transactions.getTransaction_type());
        })
                .keyBy(savingsPerAccountPerMonth::getCustomerId)
                .keyBy(savingsPerAccountPerMonth::getAccountNumber)
                .keyBy(savingsPerAccountPerMonth::getYear)
                .keyBy(savingsPerAccountPerMonth::getMonth)
                .reduce((savingsPerAccountPerMonth, savingsPerAccountPerMonth2) -> {
                    if (Objects.equals(savingsPerAccountPerMonth.getAccountNumber(), savingsPerAccountPerMonth2.getAccountNumber())) {
                        if (savingsPerAccountPerMonth.getAmount_type().equals("credit") && savingsPerAccountPerMonth2.getAmount_type().equals("credit")) {
                            savingsPerAccountPerMonth.setSavings_amount(savingsPerAccountPerMonth.getSavings_amount() + savingsPerAccountPerMonth2.getSavings_amount());
                        } else if (savingsPerAccountPerMonth.getAmount_type().equals("debit") && savingsPerAccountPerMonth2.getAmount_type().equals("debit")) {
                            savingsPerAccountPerMonth.setSavings_amount(savingsPerAccountPerMonth.getSavings_amount() + savingsPerAccountPerMonth2.getSavings_amount());
                        } else if (savingsPerAccountPerMonth.getAmount_type().equals("credit") && savingsPerAccountPerMonth2.getAmount_type().equals("debit")) {
                            savingsPerAccountPerMonth.setSavings_amount(savingsPerAccountPerMonth.getSavings_amount() - savingsPerAccountPerMonth2.getSavings_amount());
                        } else if (savingsPerAccountPerMonth.getAmount_type().equals("debit") && savingsPerAccountPerMonth2.getAmount_type().equals("credit")) {
                            savingsPerAccountPerMonth.setSavings_amount(savingsPerAccountPerMonth.getSavings_amount() - savingsPerAccountPerMonth2.getSavings_amount());
                        }
                        if (savingsPerAccountPerMonth.getSavings_amount() > 0) {
                            savingsPerAccountPerMonth.setAmount_type("credit");
                        } else {
                            savingsPerAccountPerMonth.setAmount_type("debit");
                        }
//                        return savingsPerAccountPerMonth;
                    }
                    return savingsPerAccountPerMonth;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO savingsPerAccountPerMonth (account_number, customer_id, year, month, savings_amount, amount_type) " +
                                "VALUES (?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT (account_number, customer_id, year, month) DO UPDATE SET " +
                                "account_number = EXCLUDED.account_number, " +
                                "customer_id = EXCLUDED.customer_id, " +
                                "year = EXCLUDED.year, " +
                                "month = EXCLUDED.month, " +
                                "savings_amount = EXCLUDED.savings_amount, " +
                                "amount_type = EXCLUDED.amount_type",
                        (JdbcStatementBuilder<savingsPerAccountPerMonth>) (preparedStatement, savingsPerAccountPerMonth) -> {
                            preparedStatement.setString(1, savingsPerAccountPerMonth.getAccountNumber());
                            preparedStatement.setString(2, savingsPerAccountPerMonth.getCustomerId());
                            preparedStatement.setInt(3, savingsPerAccountPerMonth.getYear());
                            preparedStatement.setInt(4, savingsPerAccountPerMonth.getMonth());
                            preparedStatement.setDouble(5, savingsPerAccountPerMonth.getSavings_amount());
                            preparedStatement.setString(6, savingsPerAccountPerMonth.getAmount_type());
                        },
                        execOptions,
                        connOptions
                )).name("Savings Per Account Per Month Sink");

        transactionStream.map(transactions -> {
            int year = transactions.getTransaction_time().toLocalDateTime().getYear();
            int month = transactions.getTransaction_time().toLocalDateTime().getMonthValue();
            return new transactionsPerCustomerPerCategoryPerMonth(transactions.getCustomerId(), transactions.getAccountNumber(), transactions.getTransaction_category(), year, month, 1, transactions.getTransaction_amount(), transactions.getTransaction_type());
        })
                .keyBy(transactionsPerCustomerPerCategoryPerMonth::getCustomerId)
                .keyBy(transactionsPerCustomerPerCategoryPerMonth::getAccountNumber)
                .keyBy(transactionsPerCustomerPerCategoryPerMonth::getCategory)
                .keyBy(transactionsPerCustomerPerCategoryPerMonth::getYear)
                .keyBy(transactionsPerCustomerPerCategoryPerMonth::getMonth)
                .reduce((transactionsPerCustomerPerCategoryPerMonth, transactionsPerCustomerPerCategoryPerMonth2) -> {
                    if (Objects.equals(transactionsPerCustomerPerCategoryPerMonth.getAccountNumber(), transactionsPerCustomerPerCategoryPerMonth2.getAccountNumber())) {
                        if (transactionsPerCustomerPerCategoryPerMonth.getAmount_type().equals("credit") && transactionsPerCustomerPerCategoryPerMonth2.getAmount_type().equals("credit")) {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount(transactionsPerCustomerPerCategoryPerMonth.getAmount() + transactionsPerCustomerPerCategoryPerMonth2.getAmount());
                        } else if (transactionsPerCustomerPerCategoryPerMonth.getAmount_type().equals("debit") && transactionsPerCustomerPerCategoryPerMonth2.getAmount_type().equals("debit")) {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount(transactionsPerCustomerPerCategoryPerMonth.getAmount() + transactionsPerCustomerPerCategoryPerMonth2.getAmount());
                        } else if (transactionsPerCustomerPerCategoryPerMonth.getAmount_type().equals("credit") && transactionsPerCustomerPerCategoryPerMonth2.getAmount_type().equals("debit")) {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount(transactionsPerCustomerPerCategoryPerMonth.getAmount() + transactionsPerCustomerPerCategoryPerMonth2.getAmount());
                        } else if (transactionsPerCustomerPerCategoryPerMonth.getAmount_type().equals("debit") && transactionsPerCustomerPerCategoryPerMonth2.getAmount_type().equals("credit")) {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount(transactionsPerCustomerPerCategoryPerMonth.getAmount() + transactionsPerCustomerPerCategoryPerMonth2.getAmount());
                        }
                        if (transactionsPerCustomerPerCategoryPerMonth.getAmount() > 0) {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount_type("credit");
                        } else {
                            transactionsPerCustomerPerCategoryPerMonth.setAmount_type("debit");
                        }
                        transactionsPerCustomerPerCategoryPerMonth.setCount(transactionsPerCustomerPerCategoryPerMonth.getCount() + transactionsPerCustomerPerCategoryPerMonth2.getCount());
//                        return transactionsPerCustomerPerCategoryPerMonth;
                    }
                    return transactionsPerCustomerPerCategoryPerMonth;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO transactionsPerCustomerPerCategoryPerMonth (account_number, customer_id, category, year, month, count, amount, amount_type) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT (account_number, customer_id, category, year, month) DO UPDATE SET " +
                                "account_number = EXCLUDED.account_number, " +
                                "customer_id = EXCLUDED.customer_id, " +
                                "category = EXCLUDED.category, " +
                                "year = EXCLUDED.year, " +
                                "month = EXCLUDED.month, " +
                                "count = EXCLUDED.count, " +
                                "amount = EXCLUDED.amount, " +
                                "amount_type = EXCLUDED.amount_type",
                        (JdbcStatementBuilder<transactionsPerCustomerPerCategoryPerMonth>) (preparedStatement, transactionsPerCustomerPerCategoryPerMonth) -> {
                            preparedStatement.setString(1, transactionsPerCustomerPerCategoryPerMonth.getAccountNumber());
                            preparedStatement.setString(2, transactionsPerCustomerPerCategoryPerMonth.getCustomerId());
                            preparedStatement.setString(3, transactionsPerCustomerPerCategoryPerMonth.getCategory());
                            preparedStatement.setInt(4, transactionsPerCustomerPerCategoryPerMonth.getYear());
                            preparedStatement.setInt(5, transactionsPerCustomerPerCategoryPerMonth.getMonth());
                            preparedStatement.setInt(6, transactionsPerCustomerPerCategoryPerMonth.getCount());
                            preparedStatement.setDouble(7, transactionsPerCustomerPerCategoryPerMonth.getAmount());
                            preparedStatement.setString(8, transactionsPerCustomerPerCategoryPerMonth.getAmount_type());
                        },
                        execOptions,
                        connOptions
                )).name("Transactions Per Customer Per Category Per Month Sink");


        transactionStream.map(transactions -> {
            int year = transactions.getTransaction_time().toLocalDateTime().getYear();
            int month =transactions.getTransaction_time().toLocalDateTime().getMonthValue();
            return new transactionInsights(transactions.getCustomerId(), transactions.getAccountNumber(), year, month, transactions.getTransaction_type(),transactions.getTransaction_amount(),1, transactions.getTransaction_amount(),transactions.getTransaction_type(), transactions.getTransaction_amount(), transactions.getTransaction_amount(), 0.0, 0.0);
        })
                .keyBy(transactionInsights::getCustomerId)
                .keyBy(transactionInsights::getAccountNumber)
                .reduce((transactionInsights, transactionInsights2) -> {
                    if (Objects.equals(transactionInsights.getAccountNumber(), transactionInsights2.getAccountNumber())) {
                        if (transactionInsights.getTransaction_type().equals("credit")) {
                            transactionInsights.setTotal_income(transactionInsights.getTransaction_amount() + transactionInsights2.getTransaction_amount());
                        } else if (transactionInsights.getTransaction_type().equals("debit")) {
                            transactionInsights.setTotal_expenses(transactionInsights.getTransaction_amount() + transactionInsights2.getTransaction_amount());
                        }
                    }
                    transactionInsights.setTotal_transactions(transactionInsights.getTotal_transactions() + transactionInsights2.getTotal_transactions());
                    transactionInsights.setTotal_savings(transactionInsights.total_income - transactionInsights.total_expenses);
                    transactionInsights.setTotal_savings_percentage((transactionInsights.total_savings / transactionInsights.total_income) * 100);
                    transactionInsights.setTotal_expenses_percentage((transactionInsights.total_expenses / transactionInsights.total_income) * 100);
                    return transactionInsights;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO transactionInsights (customer_id, account_number, year, month, transaction_type, " +
                                "transaction_amount, total_transactions, total_savings, total_savings_type, total_expenses, total_income, " +
                                "total_savings_percentage, total_expenses_percentage) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT (customer_id, account_number, year, month) DO UPDATE SET " +
                                "customer_id = EXCLUDED.customer_id, " +
                                "account_number = EXCLUDED.account_number, " +
                                "year = EXCLUDED.year, " +
                                "month = EXCLUDED.month, " +
                                "transaction_type = EXCLUDED.transaction_type, " +
                                "transaction_amount = EXCLUDED.transaction_amount, " +
                                "total_transactions = EXCLUDED.total_transactions, " +
                                "total_savings = EXCLUDED.total_savings, " +
                                "total_savings_type = EXCLUDED.total_savings_type, " +
                                "total_expenses = EXCLUDED.total_expenses, " +
                                "total_income = EXCLUDED.total_income, " +
                                "total_savings_percentage = EXCLUDED.total_savings_percentage, " +
                                "total_expenses_percentage = EXCLUDED.total_expenses_percentage",
                        (JdbcStatementBuilder<transactionInsights>) (preparedStatement, transactionInsights) -> {
                            preparedStatement.setString(1, transactionInsights.getCustomerId());
                            preparedStatement.setString(2, transactionInsights.getAccountNumber());
                            preparedStatement.setInt(3, transactionInsights.getYear());
                            preparedStatement.setInt(4, transactionInsights.getMonth());
                            preparedStatement.setString(5, transactionInsights.getTransaction_type());
                            preparedStatement.setDouble(6, transactionInsights.getTransaction_amount());
                            preparedStatement.setInt(7, transactionInsights.getTotal_transactions());
                            preparedStatement.setDouble(8, transactionInsights.getTotal_savings());
                            preparedStatement.setString(9, transactionInsights.getTotal_savings_type());
                            preparedStatement.setDouble(10, transactionInsights.getTotal_expenses());
                            preparedStatement.setDouble(11, transactionInsights.getTotal_income());
                            preparedStatement.setDouble(12, transactionInsights.getTotal_savings_percentage());
                            preparedStatement.setDouble(13, transactionInsights.getTotal_expenses_percentage());
                        },
                        execOptions,
                        connOptions
                )).name("Transaction Insights Sink");



        transactionStream.map(transactions -> {
            int year = transactions.getTransaction_time().toLocalDateTime().getYear();
            int month = transactions.getTransaction_time().toLocalDateTime().getMonthValue();
            return new transactionInsightsPerCustomerPerMerchantPerMonth(transactions.getCustomerId(), year, month, transactions.getTransaction_type(), transactions.getTransaction_amount(), transactions.getTransaction_category(), 1, transactions.getMerchant(), transactions.getTransaction_amount());
        })
                .keyBy(transactionInsightsPerCustomerPerMerchantPerMonth::getCustomer_id)
                .keyBy(transactionInsightsPerCustomerPerMerchantPerMonth::getMerchant)
                .keyBy(transactionInsightsPerCustomerPerMerchantPerMonth::getYear)
                .keyBy(transactionInsightsPerCustomerPerMerchantPerMonth::getMonth)
                .reduce((transactionInsightsPerCustomerPerMerchantPerMonth, transactionInsightsPerCustomerPerMerchantPerMonth2) -> {
                    if (Objects.equals(transactionInsightsPerCustomerPerMerchantPerMonth.getCustomer_id(), transactionInsightsPerCustomerPerMerchantPerMonth2.getCustomer_id())) {
                        if (transactionInsightsPerCustomerPerMerchantPerMonth.getMerchant().equals(transactionInsightsPerCustomerPerMerchantPerMonth2.getMerchant())) {
                            transactionInsightsPerCustomerPerMerchantPerMonth.setTotal_spend_per_Merchant(transactionInsightsPerCustomerPerMerchantPerMonth.getTotal_spend_per_Merchant() + transactionInsightsPerCustomerPerMerchantPerMonth2.getTotal_spend_per_Merchant());
                            transactionInsightsPerCustomerPerMerchantPerMonth.setTotal_transactions(transactionInsightsPerCustomerPerMerchantPerMonth.getTotal_transactions() + transactionInsightsPerCustomerPerMerchantPerMonth2.getTotal_transactions());
                        }
                    }
                    return transactionInsightsPerCustomerPerMerchantPerMonth;
//                    return transactionInsightsPerCustomerPerMerchantPerMonth2;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO transactionInsightsPerCustomerPerMerchantPerMonth (customer_id, " +
                                "year, month, transaction_type, " +
                                "transaction_amount, transaction_category, total_transactions, merchant, total_spend_per_Merchant) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON CONFLICT (customer_id, year, month) DO UPDATE SET " +
                                "customer_id = EXCLUDED.customer_id, " +
                                "year = EXCLUDED.year, " +
                                "month = EXCLUDED.month, " +
                                "transaction_type = EXCLUDED.transaction_type, " +
                                "transaction_amount = EXCLUDED.transaction_amount, " +
                                "transaction_category = EXCLUDED.transaction_category, " +
                                "total_transactions = EXCLUDED.total_transactions, " +
                                "merchant = EXCLUDED.merchant, " +
                                "total_spend_per_Merchant = EXCLUDED.total_spend_per_Merchant",
                        (JdbcStatementBuilder<transactionInsightsPerCustomerPerMerchantPerMonth>) (preparedStatement, transactionInsightsPerCustomerPerMerchantPerMonth) -> {
                            preparedStatement.setString(1, transactionInsightsPerCustomerPerMerchantPerMonth.getCustomer_id());
                            preparedStatement.setInt(2, transactionInsightsPerCustomerPerMerchantPerMonth.getYear());
                            preparedStatement.setInt(3, transactionInsightsPerCustomerPerMerchantPerMonth.getMonth());
                            preparedStatement.setString(4, transactionInsightsPerCustomerPerMerchantPerMonth.getTransaction_type());
                            preparedStatement.setDouble(5, transactionInsightsPerCustomerPerMerchantPerMonth.getTransaction_amount());
                            preparedStatement.setString(6, transactionInsightsPerCustomerPerMerchantPerMonth.getTransaction_category());
                            preparedStatement.setInt(7, transactionInsightsPerCustomerPerMerchantPerMonth.getTotal_transactions());
                            preparedStatement.setString(8, transactionInsightsPerCustomerPerMerchantPerMonth.getMerchant());
                            preparedStatement.setDouble(9, transactionInsightsPerCustomerPerMerchantPerMonth.getTotal_spend_per_Merchant());
                        },
                        execOptions,
                        connOptions

                )).name("Transaction Insights Per Mechant Spend Sink");


        transactionStream.map(transactions -> {
            int year = transactions.getTransaction_time().toLocalDateTime().getYear();
            int month = transactions.getTransaction_time().toLocalDateTime().getMonthValue();
            return new totalSpendAcrossMerchants(transactions.getMerchant(), transactions.getTransaction_amount(), 1, year, month);
        })
                .keyBy(totalSpendAcrossMerchants::getMerchant)
                .keyBy(totalSpendAcrossMerchants::getYear)
                .reduce((totalSpendAcrossMerchants, totalSpendAcrossMerchants2) -> {
                    if (Objects.equals(totalSpendAcrossMerchants.getMerchant(), totalSpendAcrossMerchants2.getMerchant())) {
                        totalSpendAcrossMerchants.setTotalSpend(totalSpendAcrossMerchants.getTotalSpend() + totalSpendAcrossMerchants2.getTotalSpend());
                        totalSpendAcrossMerchants.setCount(totalSpendAcrossMerchants.getCount() + totalSpendAcrossMerchants2.getCount());
                        return totalSpendAcrossMerchants;
                    }
                    return totalSpendAcrossMerchants2;
                })
                .addSink(JdbcSink.sink(
                        "INSERT INTO totalSpendAcrossMerchants (merchant, totalSpend, count, year, month) " +
                                "VALUES (?, ?, ?, ?, ?) " +
                                "ON CONFLICT (merchant, year, month) DO UPDATE SET " +
                                "merchant = EXCLUDED.merchant, " +
                                "totalSpend = EXCLUDED.totalSpend, " +
                                "count = EXCLUDED.count, " +
                                "year = EXCLUDED.year, " +
                                "month = EXCLUDED.month",
                        (JdbcStatementBuilder<totalSpendAcrossMerchants>) (preparedStatement, totalSpendAcrossMerchants) -> {
                            preparedStatement.setString(1, totalSpendAcrossMerchants.getMerchant());
                            preparedStatement.setDouble(2, totalSpendAcrossMerchants.getTotalSpend());
                            preparedStatement.setInt(3, totalSpendAcrossMerchants.getCount());
                            preparedStatement.setInt(4, totalSpendAcrossMerchants.getYear());
                            preparedStatement.setInt(5, totalSpendAcrossMerchants.getMonth());
                        },
                        execOptions,
                        connOptions
                )).name("Total Spend Across Merchants Sink");

        env.execute("Flink Transaction Insights");
    }
}


