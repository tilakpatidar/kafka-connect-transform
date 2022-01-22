package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ArchiveMessageTest {
    private static ArchiveMessage<SinkRecord> archiveMessage = new ArchiveMessage<>();

    @BeforeClass
    public static void setup() {
        final Map<String, Object> properties = new HashMap<>();
        properties.put("archive.schema_name", "com.github.tilakpatidar.kafka.connect.smt.ArchiveTest");
        properties.put("archive.msg_key", "key_col");
        properties.put("archive.msg_topic", "topic_col");
        properties.put("archive.msg_timestamp", "timestamp_col");
        archiveMessage.configure(properties);
    }

    @Test
    public void testArchivalMsgWithSchema() {

        final Schema kSchema = Schema.STRING_SCHEMA;
        final Schema vSchema = SchemaBuilder
                .struct()
                .field("sno", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA).build();
        Struct val = new Struct(vSchema);
        val.put("sno", "1");
        val.put("name", "foo");
        final SinkRecord sinkRecord = new SinkRecord("students", 0, kSchema, "msg-key", vSchema, val, 0, 1615542592L, TimestampType.CREATE_TIME);


        final SinkRecord resRecord = archiveMessage.apply(sinkRecord);

        final Struct resStruct = (Struct) resRecord.value();
        final String resKey = (String) resRecord.key();
        assertEquals("1", resStruct.get("sno"));
        assertEquals("foo", resStruct.get("name"));
        assertEquals("msg-key", resStruct.get("key_col"));
        assertEquals("msg-key", resKey);
        assertEquals("students", resStruct.get("topic_col"));
        assertEquals(1615542592L, resStruct.get("timestamp_col"));

        assertEquals("sno", resRecord.valueSchema().field("sno").name());
        assertEquals("name", resRecord.valueSchema().field("name").name());
        assertEquals("key_col", resRecord.valueSchema().field("key_col").name());
        assertEquals("topic_col", resRecord.valueSchema().field("topic_col").name());
        assertEquals("timestamp_col", resRecord.valueSchema().field("timestamp_col").name());


    }

    @Test
    public void testArchivalMsgWithOutSchema() {
        final Schema keySchema = Schema.STRING_SCHEMA;
        Map<String, String> values = new HashMap<>();
        values.put("sno", "1");
        values.put("name", "foo");
        final SinkRecord record = new SinkRecord("students", 0, keySchema, "msg-key", null, values, 0, 1615542592L, TimestampType.CREATE_TIME);
        final SinkRecord resultRecord = archiveMessage.apply(record);
        final Map<String, String> resultValue = (Map<String, String>) resultRecord.value();
        final String resultKey = (String) resultRecord.key();

        assertEquals("1", resultValue.get("sno"));
        assertEquals("foo", resultValue.get("name"));
        assertEquals("msg-key", resultValue.get("key_col"));
        assertEquals("msg-key", resultKey);
        assertEquals("students", resultValue.get("topic_col"));
        assertEquals(1615542592L, resultValue.get("timestamp_col"));

    }
}
