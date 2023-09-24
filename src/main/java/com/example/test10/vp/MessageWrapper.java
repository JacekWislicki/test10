package com.example.test10.vp;

import java.io.Serializable;

import org.apache.avro.specific.SpecificRecordBase;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MessageWrapper<TYPE extends SpecificRecordBase> implements Serializable {

    private static final long serialVersionUID = 1L;

    private TYPE message;
    private Metadata metadata;

    public MessageWrapper(TYPE message, String metadataString) {
        this.message = message;
        this.metadata = Metadata.fromString(metadataString);
    }

    public String getMetadataString() {
        return Metadata.toString(metadata);
    }
}
