package com.github.tilakpatidar.kafka.connect.partitioner;

import io.confluent.connect.storage.common.StorageCommonConfig;
import io.confluent.connect.storage.errors.PartitionException;
import io.confluent.connect.storage.partitioner.DefaultPartitioner;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @param <T> The type representing the field schemas.
 */
public class HeaderValuePartitioner<T> extends DefaultPartitioner<T> {
    public static final String HEADER_FIELD_NAME_CONFIG = "partitioner.header.name";
    private static final Logger log = LoggerFactory.getLogger(HeaderValuePartitioner.class);
    private List<String> headerNames;
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(HEADER_FIELD_NAME_CONFIG, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Header key names used for partitioning the record.");


    public HeaderValuePartitioner() {
    }

    public void configure(Map<String, Object> configs) {
        AbstractConfig config = new AbstractConfig(CONFIG_DEF, configs);
        this.headerNames = config.getList(HEADER_FIELD_NAME_CONFIG);
        this.delim = (String) configs.getOrDefault(StorageCommonConfig.DIRECTORY_DELIM_CONFIG, "/");
        assert !this.headerNames.isEmpty() : "Need at least one header field name for configuring HeaderValuePartitioner";
    }

    public String encodePartition(SinkRecord sinkRecord) {

        if (sinkRecord == null) {
            String errMsg = "Error encoding partition. SinkRecord is null.";
            log.error(errMsg);
            throw new PartitionException(errMsg);
        }

        List<String> partitions = new ArrayList<>(this.headerNames.size());

        for (String partitionHeader : this.headerNames) {
            boolean partitionKeyPresent = sinkRecord.headers().allWithName(partitionHeader).hasNext();
            if (!partitionKeyPresent) {
                String errMsg = String.format("Error encoding partition. No header key named %s present.", partitionHeader);
                log.error(errMsg);
                throw new PartitionException(errMsg);
            }
            Object partitionKey = sinkRecord.headers().allWithName(partitionHeader).next().value();
            partitions.add(String.format("%s=%s", partitionHeader, partitionKey));
        }
        return String.join(this.delim, partitions);
    }

    public List<T> partitionFields() {
        if (this.partitionFields == null) {
            this.partitionFields = this.newSchemaGenerator(this.config).newPartitionFields(Utils.join(this.headerNames, ","));
        }

        return this.partitionFields;
    }
}
