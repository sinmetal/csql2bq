package com.souzoh.csql2bq.model;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.coders.SerializableCoder;

import java.io.Serializable;

@AutoValue
@DefaultCoder(SerializableCoder.class)
public class SampleModel implements Serializable {

    public Long id;

    public String uuid;
}
