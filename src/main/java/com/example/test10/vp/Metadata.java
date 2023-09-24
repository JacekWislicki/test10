package com.example.test10.vp;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Data;

@Data
public final class Metadata implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final ObjectMapper mapper = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        .setSerializationInclusion(Include.NON_EMPTY);

    public static Metadata fromString(String metadataString) {
        if (StringUtils.isBlank(metadataString)) {
            return new Metadata();
        }
        try {
            return mapper.readValue(metadataString, Metadata.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error on deserialising metadata from " + metadataString, e);
        }
    }

    public static String toString(Metadata metadata) {
        if (metadata == null) {
            return "{}";
        }
        return metadata.toString();
    }

    private long vpTimestamp;

    @Override
    public String toString() {
        try {
            return mapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Error on serialising metadata from " + super.toString(), e);
        }
    }
}
