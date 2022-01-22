package com.github.tilakpatidar.kafka.connect.smt;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public abstract class CloneFieldsToHeader<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Clone record fields into headers of a kafka record";
    private static final String PURPOSE = "Clone record fields into headers of a kafka record";
    private static final String FIELD_NAME = "clonefieldstoheader.from";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_NAME, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Field names to clone into header.");

    private List<String> fieldNames;

    @Override
    public void configure(Map<String, ?> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        fieldNames = config.getList(FIELD_NAME);
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
        for (String fieldName : fieldNames) {
            record.headers().addString(fieldName, String.valueOf(updatedValue.get(fieldName)));
        }
        return record;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record));
        for (String fieldName : fieldNames) {
            record.headers().addString(fieldName, String.valueOf(value.get(fieldName)));
        }
        return record;
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

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> headers);

    public static class Key<R extends ConnectRecord<R>> extends CloneFieldsToHeader<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> headers) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp(), headers);
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CloneFieldsToHeader<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue, Iterable<Header> headers) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp(), headers);
        }
    }
}
