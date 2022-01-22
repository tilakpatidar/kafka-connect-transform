package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
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


public abstract class CloneField<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Creates a clone of a field";
    private static final String FIELD_NAME = "clonefield.from";
    private static final String NEW_FIELD_NAME = "clonefield.to";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field name")
            .define(NEW_FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "New field name");

    private String fieldName;
    private String newFieldName;

    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        fieldName = config.getString(FIELD_NAME);
        newFieldName = config.getString(NEW_FIELD_NAME);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record));
        final Map<String, Object> updatedValue = new HashMap<>(value);
        updatedValue.put(newFieldName, value.get(fieldName));
        return newRecord(record, null, updatedValue);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record));

        Schema updatedSchema = schemaUpdateCache.get(value.schema());

        if (updatedSchema == null) {
            updatedSchema = addNewFieldSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        updatedValue.put(newFieldName, value.get(fieldName));

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema addNewFieldSchema(Schema schema) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(schema.name());

        for (Field field : schema.fields()) {
            schemaBuilder = schemaBuilder.field(field.name(), field.schema());
        }


        schemaBuilder.field(newFieldName, schema.field(fieldName).schema());

        return schemaBuilder.build();
    }


    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    private static Struct requireStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new DataException("Only Struct objects supported for [" + CloneField.class.getName() + "], found: " + nullSafeClassName(value));
        }
        return (Struct) value;
    }

    private static Map<String, Object> requireMap(Object value) {
        if (!(value instanceof Map)) {
            throw new DataException("Only Map objects supported for [" + CloneField.class.getName() + "], found: " + nullSafeClassName(value));
        }
        return (Map<String, Object>) value;
    }

    private static String nullSafeClassName(Object x) {
        return x == null ? "null" : x.getClass().getName();
    }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CloneField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CloneField<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
