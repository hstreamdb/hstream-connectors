# HStream Connectors

Connector plugins and toolkits for HStream IO.

## Toolkits

Toolkits are libraries for developers to create new connector plugins,
now we have implemented the [java-toolkit](java-toolkit),
based on java-toolkit,
you only need to implement a couple of interfaces to create a connector plugin
instead of caring about offset, batch, protocol messages processing, etc.

## source-debezium
Debezium is a great CDC tool for databases,
we use debezium engine to synchronize data from databases,
source-debezium is not a real connector plugin,
it is the general database source library for building the real database connector plugins
including source-mysql, source-postgresql, source-sqlserver, etc.

## sink-jdbc
We use jdbc as a general interface to write data from HStreamDB streams to sink databases.
like source-debezium, sink-jdbc is not an actual connector plugin either,
we use sink-jdbc to build the database connectors
including sink-mysql, sink-postgresql, etc.

sink-jdbc uses the ``UPSERT`` statement to implement ``INSERT`` and ``UPDATE`` for idempotence,
so it can deal with the duplicated records caused by resending.
