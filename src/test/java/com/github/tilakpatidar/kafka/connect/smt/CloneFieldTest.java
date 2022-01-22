package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class CloneFieldTest {

    private CloneField<SinkRecord> cloneField = new CloneField.Value<>();

    @Test
    public void testCloningOfField() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("clonefield.from", "billingDate");
        properties.put("clonefield.to", "deliveryDate");
        cloneField.configure(properties);

        final Schema schema = SchemaBuilder.struct().field("sno", Schema.STRING_SCHEMA).field("billingDate", Schema.STRING_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("sno", "1");
        struct.put("billingDate", "foo");
        final SinkRecord sinkRecord = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
        final SinkRecord resRecord = cloneField.apply(sinkRecord);
        final Struct resStruct = (Struct) resRecord.value();

        assertEquals("foo", resStruct.get("billingDate"));
        assertEquals("foo", resStruct.get("deliveryDate"));
        assertEquals("deliveryDate", resRecord.valueSchema().field("deliveryDate").name());
        assertEquals("billingDate", resRecord.valueSchema().field("billingDate").name());
        assertEquals("sno", resRecord.valueSchema().field("sno").name());
        assertEquals(Schema.Type.STRING, resRecord.valueSchema().field("deliveryDate").schema().type());
        assertEquals(Schema.Type.STRING, resRecord.valueSchema().field("billingDate").schema().type());
    }

    @Test
    public void testCloningOfFieldWithAnotherDataType() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("clonefield.from", "billingDate");
        properties.put("clonefield.to", "deliveryDate");
        cloneField.configure(properties);

        final Schema schema = SchemaBuilder.struct().field("sno", Schema.STRING_SCHEMA).field("billingDate", Schema.INT32_SCHEMA).build();
        Struct struct = new Struct(schema);
        struct.put("sno", "1");
        struct.put("billingDate", 123);
        final SinkRecord rec = new SinkRecord("test_topic", 0, null, null, schema, struct, 0);
        final SinkRecord resRecord = cloneField.apply(rec);
        final Struct resStruct = (Struct) resRecord.value();

        assertEquals(123, resStruct.get("billingDate"));
        assertEquals(123, resStruct.get("deliveryDate"));
        assertEquals("deliveryDate", resRecord.valueSchema().field("deliveryDate").name());
        assertEquals("billingDate", resRecord.valueSchema().field("billingDate").name());
        assertEquals("sno", resRecord.valueSchema().field("sno").name());
        assertEquals(Schema.Type.INT32, resRecord.valueSchema().field("deliveryDate").schema().type());
        assertEquals(Schema.Type.INT32, resRecord.valueSchema().field("billingDate").schema().type());
    }

}