package org.myorg.quickstart;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class SensorDataStatisticsSchema implements SerializationSchema<SensorDataStatistics> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public byte[] serialize(SensorDataStatistics sensorDataStatistics) {
        try {
            return objectMapper.writeValueAsBytes(sensorDataStatistics);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
