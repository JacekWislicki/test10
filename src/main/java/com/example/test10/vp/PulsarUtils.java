package com.example.test10.vp;

import org.apache.pulsar.client.api.Schema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PulsarUtils {

    public static final String SCHEMA = "_VP_SCHEMA_";
    public static final String METADATA = "_VP_METADATA_";

    public static <T> String getAvroSchemaString(Class<T> clazz) {
        Schema<T> schema = Schema.AVRO(clazz);
        return schema.getNativeSchema()
            .orElseThrow(() -> new RuntimeException("Cannot get Avro schema from Pulsar schema for " + clazz))
            .toString();
    }
}
