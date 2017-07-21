# ChanaMQ Server

[ChanaMQ](http://https://github.com/qingmang-team/chana-mq) is an Akka based, AMQP messaging broker. It currently supports:

 * AMQP 0-9-1

## Status

 * ChanaMQ is still under the alpha phase.
 * Methods/Features are not supported yet:
   * Connection.SecureOk
   * Channel.Flow
   * Exchange.Bind / Exchange.Unbind
   * Basic.Reject / Basic.Nack / Basic.RecoverAsync / Basic.Recover
   * Access.Request
   * Tx.Select / Tx.Commit / Tx.Rollback
   * Confirm.Select

## Building From Source and Packaging

 * sbt clean chana-mq-server/dist

## Installation

 * To enable persistent feature, you need to install Cassandra first, and create chanamq space and tables as [create-cassantra.cql](https://github.com/qingmang-team/chanamq/blob/master/chana-mq-server/src/main/resources/create-cassantra.cql)

## License

ChanaMQ server is licensed under the [Apache License version 2.0](LICENSE-APACHE2).

