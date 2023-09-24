package com.example.test10;

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

import com.example.test10.model.TestMessage;

class OutFunction implements Function<TestMessage, Void> {

    @Override
    public Void process(TestMessage input, Context context) throws PulsarClientException {
        String fromTopic = context.getCurrentRecord().getTopicName().orElse(null);
        System.out.println("Received message from: " + fromTopic);

        return null;
    }
}
