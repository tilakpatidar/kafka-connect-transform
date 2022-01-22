package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CloneFieldsToHeaderTest {

    private CloneFieldsToHeader<SinkRecord> cloneFieldsToHeader = new CloneFieldsToHeader.Value<>();

    @Test
    public void testCloningOfFieldToHeader() {
        final Map<String, Object> props = new HashMap<>();
        props.put("clonefieldstoheader.from", Arrays.asList("billingDate"));
        cloneFieldsToHeader.configure(props);

        final Schema recSchema = SchemaBuilder.struct().field("sno", Schema.STRING_SCHEMA).field("billingDate", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(recSchema);
        struct.put("sno", "1");
        struct.put("billingDate", "foo");

        final SinkRecord rec = new SinkRecord("test_topic", 0, null, null, recSchema, struct, 0);
        final SinkRecord resRecord = cloneFieldsToHeader.apply(rec);
        final Struct resStruct = (Struct) resRecord.value();

        assertEquals("foo", resStruct.get("billingDate"));
        assertEquals("billingDate", resRecord.valueSchema().field("billingDate").name());
        assertEquals("sno", resRecord.valueSchema().field("sno").name());
        assertEquals(Schema.Type.STRING, resRecord.valueSchema().field("billingDate").schema().type());

        assertEquals("billingDate", resRecord.headers().allWithName("billingDate").next().key());
        assertEquals("foo", resRecord.headers().allWithName("billingDate").next().value());
    }

}