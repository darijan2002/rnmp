package org.myorg.quickstart;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SensorDataSchema implements DeserializationSchema<SensorData> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public SensorData deserialize(byte[] bytes) throws IOException {
        return objectMapper.readValue(bytes, SensorData.class);
    }

    @Override
    public boolean isEndOfStream(SensorData sensorData) {
        return false;
    }

    @Override
    public TypeInformation<SensorData> getProducedType() {
        return TypeInformation.of(SensorData.class);
    }
}
