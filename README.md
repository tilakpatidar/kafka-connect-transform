## kafka-connect-transform

This project contains kafka-connect related custom transformation.

### Available components

| Component Type           | Class Name                                                               | Description                                                                                                                                        | Configuration                                                                                                                                                                                                                                                                                              |
|--------------------------|--------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Partitioner              | com.github.tilakpatidar.kafka.connect.partitioner.HeaderValuePartitioner | Partition records based on key/value pairs present in record header.                                                                               | `partition.header.name`: Comma separated list of header keys  `directory.delim`: Delimiter to use when encoding partition value. Default /                                                                                                                                                                 |
| Single Message Transform | com.github.tilakpatidar.kafka.connect.smt.ArchiveMessage                 | Copies message key, topic name and message timestamp to the original message value for archiving the entire kafka key-value message as one record. | `archive.schema_name`: Name of the schema object.  `archive.msg_key`: Name of column in which kafka message key will be stored.   `archive.msg_topic`: Name of column in which kafka topic name will be stored.   `archive.msg_timestamp`: Name of column in which kafka message timestamp will be stored. |
| Single Message Transform | com.github.tilakpatidar.kafka.connect.smt.CloneField                      | Create clone of field with different name.                                                                                                        | `clonefield.from` : Name of an existing field to clone value from. `clonefield.to`: Name of the new field to which value from `clonefield.from` field will be pasted.                                                                                                                                          |
| Single Message Transform | com.github.tilakpatidar.kafka.connect.smt.CloneFieldsToHeader             | Clone record fields into headers of a kafka record                                                                                                | `clonefieldstoheader.from` : Field name or comma separated string for multiple fields.                                                                                                                                                                                                                      |

### Installation 

#### For confluent kafka connect

Download the zip file available in the releases.

```shell
confluent-hub install <zip file>
```

#### For apache kafka connect

Download the jar file available in the releases. Place it in your `plugin.path`


