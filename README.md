# DM-aiomqtt

## Urls

* [PyPI](https://pypi.org/project/dm-aiomqtt)
* [GitHub](https://github.com/MykhLibs/dm-aiomqtt)

## Usage

### Example

```python
from dm_aiomqtt import DMAioMqttClient
import asyncio


# create handler for 'test' topic
async def test_topic_handler(publish: DMAioMqttClient.publish, topic: str, payload: str) -> None:
    print(f"Received message from {topic}: {payload}")
    publish("test/success", payload=True)


async def main():
    # create client
    mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

    # add handler for 'test' topic
    mqtt_client.add_topic_handler("test", test_topic_handler)

    # start connection
    await mqtt_client.start()

    # publish
    mqtt_client.publish("test", payload="Hello World!", sent_logging=True)

    # other code (for example, endless waiting)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

You can also start with a block thread

```python
await mqtt_client.start_forever()
```

### TLS connection

* NOT required client certificate

   ```python
   mqtt_client = DMAioMqttClient(
       host="localhost",
       port=8883,
       ca_crt="ssl/ca.crt"
   )
   ```

* REQUIRED client certificate

   ```python
   mqtt_client = DMAioMqttClient(
       host="localhost",
       port=8883,
       ca_crt="ssl/ca.crt",
       client_crt="ssl/client.crt",
       client_key="ssl/client.key"
   )
   ```

### RESEND_NOT_SUCCESS_MESSAGES

Set this parameter `resend_not_success_messages=True` when create mqtt client

   ```python
   mqtt_client = DMAioMqttClient(
       host="localhost",
       port=1883,
       resend_not_success_messages=True
   )
   ```

Now, in case of loss of connection, all messages that were sent during this period will be re-sent as soon as the connection appears again.

### Set custom logger

_If you want set up custom logger parameters_

```python
from dm_aiomqtt import DMAioMqttClient
from dm_logger import FormatterConfig


# set up custom logger for all clients
DMAioMqttClient.set_logger_params(
   {
      "name": "my_name",
      "formatter_config": FormatterConfig(
         show_datetime=False,
      )
   }
)
```

See more about DMLogger [here](https://github.com/MykhLibs/dm-logger)


### Publish method parameters

| Parameter         | Type               | Default Value | Description                               |
|-------------------|--------------------|---------------|-------------------------------------------|
| `topic`           | `str`              | (required)    | Topic name                                |
| `payload`         | `str`              | `"DEBUG"`     | Content to send                           |
| `qos`             | `0` \| `1` \| `2`  | `True`        | MQTT QoS                                  |
| `payload_to_json` | `bool` \| `"auto"` | `True`        | Whether to convert content to JSON        |
| `sent_logging`    | `bool`             | `False`       | Whether to print the sending notification |
| `error_logging`   | `bool`             | `False`       | Whether to print a send error warning     |

### Run in Windows

_If you run async code in **Windows**, set correct selector_

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```
