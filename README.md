# DM-aiomqtt

## Urls

* [PyPI](https://pypi.org/project/dm-aiomqtt)
* [GitHub](https://github.com/DIMKA4621/dm-aiomqtt)

## Usage

### Warning

For correct operation of the client, **_readwrite_** access to `ping/#` topic is **REQUIRED**.
Improved system for responding to loss of connection, will use this topic to send **ping** messages.

(Format of ping messages - topic: `ping/[uuid4]`, payload: `1`)

### Run in Windows

_If you run async code in **Windows**, set correct selector_

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### Example

```python
from dm_aiomqtt import DMAioMqttClient
import asyncio


# create handler for 'test' topic
async def test_topic_handler(publish: DMAioMqttClient.publish, topic: str, payload: str) -> None:
    print(f"Received message from {topic}: {payload}")
    await publish("test/success", payload=True)


async def main():
    # create client
    mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

    # add handler for 'test' topic
    mqtt_client.add_topic_handler("test", test_topic_handler)

    # start connection
    await mqtt_client.start()

    # publish
    await mqtt_client.publish("test", payload="Hello World!", sent_logging=True)

    # other code (for example, endless waiting)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### Set custom logger

_If you want set up custom logger_

```python
from dm_aiomqtt import DMAioMqttClient


# create custom logger
class MyLogger:
    def debug(self, message):
        pass

    def info(self, message):
        pass

    def warning(self, message):
        print(message)

    def error(self, message):
        print(message)


# set up custom logger for all clients
DMAioMqttClient.set_logger(MyLogger())
```

### Publish method parameters

| Parameter          | Type               | Default Value | Description                               |
|--------------------|--------------------|---------------|-------------------------------------------|
| `topic`            | `str`              | (required)    | Topic name                                |
| `payload`          | `str`              | `"DEBUG"`     | Content to send                           |
| `qos`              | `0` \| `1` \| `2`  | `True`        | MQTT QoS                                  |
| `payload_to_json`  | `bool` \| `"auto"` | `True`        | Whether to convert content to JSON        |
| `sent_logging`     | `bool`             | `False`       | Whether to print the sending notification |
| `not_sent_logging` | `bool`             | `False`       | Whether to print a send error warning     |
