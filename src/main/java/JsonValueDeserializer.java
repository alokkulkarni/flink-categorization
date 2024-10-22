import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class JsonValueDeserializer implements DeserializationSchema<Transactions> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void open(InitializationContext context) throws Exception {
        DeserializationSchema.super.open(context);
    }

    @Override
    public Transactions deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, Transactions.class);
    }

    @Override
    public boolean isEndOfStream(Transactions transactions) {
        return false;
    }

    @Override
    public TypeInformation<Transactions> getProducedType() {
        return TypeInformation.of(Transactions.class);
    }
}
