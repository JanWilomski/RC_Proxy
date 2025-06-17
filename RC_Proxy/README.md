# RC_Proxy

RC_Proxy is a transparent proxy server that sits between RC clients (RC_GUI) and the RC server, providing message buffering and distribution via RabbitMQ.

## Features

- **Transparent Proxy**: All RC protocol messages are forwarded bidirectionally
- **Message Buffering**: CCG messages are published to RabbitMQ queues
- **High Performance**: Asynchronous TCP handling with configurable connection limits
- **Message Processing**: Parses and classifies RC messages for intelligent routing
- **Health Monitoring**: Built-in health checks and connection monitoring
- **Configurable**: Flexible configuration via appsettings.json

## Architecture

```
RC_GUI → RC_Proxy → RC_Server
            ↓
RabbitMQ (CCG Messages Queue)
```

## Configuration

Edit `appsettings.json`:

```json
{
  "ProxyConfiguration": {
    "RcServerHost": "127.0.0.1",
    "RcServerPort": 19083,
    "ProxyListenPort": 19084
  },
  "RabbitMq": {
    "HostName": "localhost",
    "ExchangeName": "rc_messages",
    "CcgQueueName": "ccg_messages"
  }
}
```

## Usage

1. Start RabbitMQ server
2. Run RC_Proxy: `dotnet run`
3. Configure RC_GUI to connect to proxy port (19084)
4. RC_GUI will receive all messages normally
5. CCG messages are also available in RabbitMQ queue

## Message Flow

- **All Messages**: Forwarded transparently between client and server
- **CCG Messages**: Additionally published to `ccg_messages` queue
- **Connection Events**: Published to `all_rc_messages` queue

## Monitoring

- Health check endpoint (if enabled)
- Connection statistics logging
- Message throughput monitoring