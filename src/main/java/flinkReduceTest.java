import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flinkReduceTest {


    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        String filePath = "/Users/alokkulkarni/Documents/Development/banking/flink/src/main/resources/avg.txt";

        SingleOutputStreamOperator<Tuple2<String, Double>> avg = env.readTextFile(filePath)
                .map(new Splitter())
                .keyBy(0)
                .reduce(new Reducer())
                .map(new Processor());
        avg.print();
        env.execute();
    }

    private static final class Reducer implements ReduceFunction<Tuple5<String,String,String, Integer, Integer>> {

        @Override
        public Tuple5<String, String, String, Integer, Integer> reduce(Tuple5<String, String, String, Integer, Integer> t1, Tuple5<String, String, String, Integer, Integer> t2) throws Exception {
            return new Tuple5<>(t1.f0, t1.f1, t1.f2, t1.f3 + t2.f3, t1.f4 + t2.f4);
        }
    }

    private static final class Splitter implements MapFunction<String, Tuple5<String, String, String, Integer, Integer>> {
        @Override
        public Tuple5<String, String, String, Integer, Integer> map(String value) {
            String[] tokens = value.split(",");
            return new Tuple5<>(tokens[1], tokens[2], tokens[3], Integer.parseInt(tokens[4]), 1);
        }
    }

    private static final class Processor implements MapFunction<Tuple5<String, String, String, Integer, Integer>, Tuple2<String, Double>> {

        @Override
        public Tuple2<String, Double> map(Tuple5<String, String, String, Integer, Integer> t1) throws Exception {
            return new Tuple2<>(t1.f0, t1.f3 * 1.0 / t1.f4);
        }
    }
}
