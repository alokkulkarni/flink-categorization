import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class flinkAggregationTest {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        String filePath = "/Users/alokkulkarni/Documents/Development/banking/flink/src/main/resources/agg.txt";

        //Aggregation Operations in flink
        env.readTextFile(filePath)
                .keyBy(0)
                .sum(3)
                .print();

        // this aggregation operation consider min of column specified in the argument and return the row with other columns as random or garbage values
        env.readTextFile(filePath)
                .keyBy(0)
                .min(3)
                .print();

        // this aggregation operation consider max of column specified in the argument and return the row with other columns as random or garbage values
        env.readTextFile(filePath)
                .keyBy(0)
                .max(3)
                .print();

        // this aggregation operation consider min of column specified in the argument
        env.readTextFile(filePath)
                .keyBy(0)
                .minBy(3)
                .print();

        // this aggregation operation consider max of column specified in the argument
        env.readTextFile(filePath)
                .keyBy(0)
                .maxBy(3)
                .print();
    }
}
