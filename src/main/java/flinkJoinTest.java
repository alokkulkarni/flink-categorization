import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

public class flinkJoinTest {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello, World!");
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        String personFilePath = "/Users/alokkulkarni/Documents/Development/banking/flink/src/main/resources/person.txt";
        String locationFilePath = "/Users/alokkulkarni/Documents/Development/banking/flink/src/main/resources/location.txt";

        MapOperator<String, Tuple2<Integer, String>> personMap = env.readTextFile(personFilePath)
                .map(new Tokenizer());

        MapOperator<String, Tuple2<Integer, String>> locationMap = env.readTextFile(locationFilePath)
                .map(new Tokenizer());

        System.out.println("Inner Join");
        personMap.join(locationMap)
                .where(0)
                .equalTo(0)
                .with(new Joiner())
                .print();

        System.out.println("Left Outer Join");
        personMap.leftOuterJoin(locationMap)
                .where(0)
                .equalTo(0)
                .with(new Joiner())
                .print();

        System.out.println("Right Outer Join");
        personMap.rightOuterJoin(locationMap)
                .where(0)
                .equalTo(0)
                .with(new Joiner())
                .print();

        System.out.println("Full Outer Join");
        personMap.fullOuterJoin(locationMap)
                .where(0)
                .equalTo(0)
                .with(new Joiner())
                .print();
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<Integer, String>> {
        @Override
        public Tuple2<Integer, String> map(String value) {
            String[] tokens = value.split(",");
            return new Tuple2<>(Integer.parseInt(tokens[0]), tokens[1]);
        }
    }

    public static final class Joiner implements JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>> {
        @Override
        public Tuple3<Integer, String, String> join(Tuple2<Integer, String> person, Tuple2<Integer, String> location) {
            if (location == null)
                return new Tuple3<>(person.f0, person.f1, "NULL");
            if (person == null)
                return new Tuple3<>(location.f0, "NULL", location.f1);
            return new Tuple3<>(person.f0, person.f1, location.f1);
        }
    }
}
