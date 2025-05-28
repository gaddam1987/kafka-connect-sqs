package com.nordstrom.kafka.connect.converter;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;

import java.util.Map;

public interface SinkMessageBuilder {
    /**
     * Builds a message from the given SinkRecord.
     *
     * @param record the SinkRecord to convert
     * @return a message in the format expected by the target system
     */
    String buildMessage(SinkRecord record);

    /**
     * Builds message attributes from the given Headers.
     *
     * @param headers the Headers to convert
     * @return a map of message attributes
     */
    Map<String, MessageAttributeValue> buildMessageAttributes(Headers headers);
}
