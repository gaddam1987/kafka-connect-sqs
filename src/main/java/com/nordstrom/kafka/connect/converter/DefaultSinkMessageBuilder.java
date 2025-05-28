package com.nordstrom.kafka.connect.converter;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.Map;

public class DefaultSinkMessageBuilder implements SinkMessageBuilder {
    @Override
    public String buildMessage(SinkRecord record) {
        if (record.value() == null) {
            return "";
        }
        // Convert the record value to a string representation
        return  record.value().toString();
    }

    @Override
    public Map<String, MessageAttributeValue> buildMessageAttributes(Headers headers) {
        return Map.of();
    }
}
