import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;

public class flinkTest {
    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        String filePath = "/Users/alokkulkarni/Documents/Development/banking/flink/src/main/resources/wc.txt";

        env.readTextFile(filePath)
                .filter(token -> token.startsWith("N"))
                .map(new Tokenizer())
                .groupBy(0)
                .sum(1)
                .print();
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        @Override
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<>(value, 1);
        }
    }
}
