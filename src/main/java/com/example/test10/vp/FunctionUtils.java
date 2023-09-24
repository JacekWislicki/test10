package com.example.test10.vp;

import java.util.concurrent.TimeUnit;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.api.Context;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FunctionUtils {

    @Data
    public static class Delay {

        private final long delay;
        private final TimeUnit unit;
    }

    public static <T extends SpecificRecordBase> void produceDifferentType(MessageWrapper<T> wrapper, Class<T> messageClass, Context context,
        String topic) throws PulsarClientException {
        produceDifferentType(wrapper, messageClass, context, topic, null);
    }

    public static <T extends SpecificRecordBase> void produceDifferentType(MessageWrapper<T> wrapper, Class<T> messageClass, Context context,
        String topic, Delay delay) throws PulsarClientException {
        TypedMessageBuilder<T> builder = prepareMessageBuilder(context, wrapper, topic, messageClass);
        if (delay != null) {
            builder = builder.deliverAfter(delay.getDelay(), delay.getUnit());
        }

        builder.send();
    }

    public static <T extends SpecificRecordBase> void produce(MessageWrapper<T> wrapper, Context context, String topic, Delay delay,
        Class<T> messageClass) throws PulsarClientException {
        TypedMessageBuilder<T> builder = prepareMessageBuilder(context, wrapper, topic, messageClass);
        if (delay != null) {
            builder = builder.deliverAfter(delay.getDelay(), delay.getUnit());
        }
        builder.send();
    }

    private static <T extends SpecificRecordBase> TypedMessageBuilder<T> prepareMessageBuilder(Context context, MessageWrapper<T> wrapper,
        String topic, Class<T> clazz) throws PulsarClientException {
        return context.newOutputMessage(topic, Schema.AVRO(clazz))
            .property(PulsarUtils.SCHEMA, PulsarUtils.getAvroSchemaString(clazz))
            .property(PulsarUtils.METADATA, wrapper.getMetadataString())
            .value(wrapper.getMessage());
    }

    public static <T extends SpecificRecordBase> void produce(MessageWrapper<T> wrapper, Context context, String topic, Class<T> clazz)
        throws PulsarClientException {
        produce(wrapper, context, topic, null, clazz);
    }

    public static <T extends SpecificRecordBase> void produce(MessageWrapper<T> wrapper, Context context, Class<T> clazz)
        throws PulsarClientException {
        produce(wrapper, context, context.getOutputTopic(), null, clazz);
    }
}
