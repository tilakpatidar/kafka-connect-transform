package com.github.tilakpatidar.kafka.connect.partitioner;

import io.confluent.connect.storage.errors.PartitionException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class HeaderValuePartitionerTest {

    private final HeaderValuePartitioner<SinkRecord> headerValuePartitioner = new HeaderValuePartitioner<>();

    @Test
    public void testPartitioningOfRecord() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(HeaderValuePartitioner.HEADER_FIELD_NAME_CONFIG, Arrays.asList("y", "m", "d"));
        headerValuePartitioner.configure(properties);

        final Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("name", "dave");

        final SinkRecord record = new SinkRecord("testing_topic", 0, null, null, schema, struct, 0);
        record.headers().addString("y", "2021").addString("m", "06").addString("d", "15");
        final String partitionPath = headerValuePartitioner.encodePartition(record);

        assertEquals("y=2021/m=06/d=15", partitionPath);
    }

    @Test
    public void testPartitioningOfRecordsWhenRecordIsNull() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(HeaderValuePartitioner.HEADER_FIELD_NAME_CONFIG, Arrays.asList("y", "m", "d"));
        headerValuePartitioner.configure(properties);

        Exception exception1 = assertThrows(PartitionException.class, () -> headerValuePartitioner.encodePartition(null));

        assertTrue(exception1.getMessage().contains("Error encoding partition. SinkRecord is null."));

    }

    @Test
    public void testPartitioningOfRecordsWhenRecordHeaderDoesNotHaveRequiredValuesInHeader() {
        final Map<String, Object> props = new HashMap<>();
        props.put(HeaderValuePartitioner.HEADER_FIELD_NAME_CONFIG, Arrays.asList("y", "m", "d"));
        headerValuePartitioner.configure(props);

        final Schema recordSchema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(recordSchema);
        struct.put("name", "dave");

        final SinkRecord record = new SinkRecord("testing_topic", 0, null, null, recordSchema, struct, 0);
        record.headers().addString("y", "2021").addString("d", "15");


        Exception exception = assertThrows(PartitionException.class, () -> headerValuePartitioner.encodePartition(record));

        assertTrue(exception.getMessage().contains("Error encoding partition. No header key named m present."));
    }

    @Test
    public void testConfiguringPartitionerWithNoHeaderFields() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put(HeaderValuePartitioner.HEADER_FIELD_NAME_CONFIG, Collections.emptyList());


        AssertionError exception1 = assertThrows(AssertionError.class, () -> headerValuePartitioner.configure(properties));

        assertTrue(exception1.getMessage().contains("Need at least one header field name for configuring HeaderValuePartitioner"));

    }

}