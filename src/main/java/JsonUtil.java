import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String convertTransactionToJson(Transactions transactions) {
        try {
            return objectMapper.writeValueAsString(transactions);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

}
