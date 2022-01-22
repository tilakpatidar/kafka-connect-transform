package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

public class ArchiveMessage<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final String OVERVIEW_DOC = "Add key, topic name, timestamp, offset to the message value. Useful for dumping kafka topic data using kafka connect for archival or analytical querying.";
    private static final String SCHEMA_NAMESPACE = "archive.schema_name";
    private static final String SCHEMA_NAMESPACE_DOC = "Name of the schema object.";
    private static final String MSG_KEY = "archive.msg_key";
    private static final String MSG_KEY_DOC = "Name of column in which kafka message key will be stored.";
    private static final String MSG_TOPIC = "archive.msg_topic";
    private static final String MSG_TOPIC_DOC = "Name of column in which kafka topic name will be stored.";
    private static final String MSG_TIMESTAMP = "archive.msg_timestamp";
    private static final String MSG_TIMESTAMP_DOC = "Name of column in which kafka message timestamp will be stored.";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SCHEMA_NAMESPACE, ConfigDef.Type.STRING, "com.github.tilakpatidar.kafka.connect.smt.Archive", ConfigDef.Importance.HIGH, SCHEMA_NAMESPACE_DOC)
            .define(MSG_KEY, ConfigDef.Type.STRING, MSG_KEY, ConfigDef.Importance.HIGH, MSG_KEY_DOC)
            .define(MSG_TOPIC, ConfigDef.Type.STRING, MSG_TOPIC, ConfigDef.Importance.HIGH, MSG_TOPIC_DOC)
            .define(MSG_TIMESTAMP, ConfigDef.Type.STRING, MSG_TIMESTAMP, ConfigDef.Importance.HIGH, MSG_TIMESTAMP_DOC);

    private String schemaNamespace;
    private String msgKeyColumn;
    private String msgTopicColumn;
    private String msgTimestampColumn;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        this.schemaNamespace = config.getString(SCHEMA_NAMESPACE);
        this.msgKeyColumn = config.getString(MSG_KEY);
        this.msgTopicColumn = config.getString(MSG_TOPIC);
        this.msgTimestampColumn = config.getString(MSG_TIMESTAMP);
    }

    @Override
    public R apply(R r) {
        if (r.valueSchema() == null) {
            return applySchemaless(r);
        } else {
            return applyWithSchema(r);
        }
    }

    private R applyWithSchema(R r) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(this.schemaNamespace)
                .field(this.msgKeyColumn, r.keySchema())
                .field(this.msgTopicColumn, Schema.STRING_SCHEMA)
                .field(this.msgTimestampColumn, Schema.INT64_SCHEMA);

        for (Field field : r.valueSchema().schema().fields()) {
            schemaBuilder = schemaBuilder.field(field.name(), field.schema());
        }


        final Schema schema = schemaBuilder.build();
        final Struct value = requireStruct(r.value());

        Struct newValue = new Struct(schema)
                .put(this.msgKeyColumn, r.key())
                .put(this.msgTopicColumn, r.topic())
                .put(this.msgTimestampColumn, r.timestamp());


        for (Field field : r.valueSchema().schema().fields()) {
            newValue = newValue.put(field.name(), value.get(field));
        }

        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, newValue, r.timestamp());
    }

    @SuppressWarnings("unchecked")
    private R applySchemaless(R r) {

        final Map<String, Object> archiveValue = new HashMap<>();

        final Map<String, Object> value = (Map<String, Object>) r.value();

        archiveValue.put(this.msgKeyColumn, r.key());
        archiveValue.put(this.msgTopicColumn, r.topic());
        archiveValue.put(this.msgTimestampColumn, r.timestamp());
        archiveValue.putAll(value);

        return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), null, archiveValue, r.timestamp());
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {

    }


    private static Struct requireStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + ArchiveMessage.class.getName() + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }


}
