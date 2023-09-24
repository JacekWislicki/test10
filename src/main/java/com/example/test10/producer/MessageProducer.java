package com.example.test10.producer;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import com.example.test10.Config;
import com.example.test10.model.TestMessage;
import com.example.test10.model.Type;

public class MessageProducer extends AbstractProducer<TestMessage> {

    MessageProducer(String topic) throws PulsarClientException {
        super(topic, Schema.AVRO(TestMessage.class));
    }

    private TestMessage buildTestMessage() {
        TestMessage message = new TestMessage();
        message.setEventIdentifier("eventId");
        message.setType(Type.TYPE_B);
        message.setActive(true);
        return message;
    }

    public static void main(String[] args) throws PulsarClientException {
        try (MessageProducer producer = new MessageProducer(Config.IN_TOPIC);) {
            TestMessage message = producer.buildTestMessage();
            producer.produce(message);
        }
    }
}
