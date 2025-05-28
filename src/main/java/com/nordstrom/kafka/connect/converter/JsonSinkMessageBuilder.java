package com.nordstrom.kafka.connect.converter;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class JsonSinkMessageBuilder implements SinkMessageBuilder {
    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String buildMessage(SinkRecord record) {
        if (record.value() == null) {
            return "";
        }
        try {
            return objectMapper.writer().writeValueAsString(record.value());
        } catch (Exception e) {
            throw new RuntimeException("Failed to convert record value to JSON", e);
        }
    }

    @Override
    public Map<String, MessageAttributeValue> buildMessageAttributes(Headers headers) {
        return Map.of();
    }
}
