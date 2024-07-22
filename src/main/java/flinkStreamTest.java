import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class flinkStreamTest {

    private static final Logger logger = LoggerFactory.getLogger(flinkStreamTest.class);

    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = env.socketTextStream("localhost", 9999)
                .filter(token -> token.startsWith("N"))
                .map(new flinkTest.Tokenizer())
                .keyBy(0)
                .sum(1);
        sum.print();
        env.execute();
    }

}
