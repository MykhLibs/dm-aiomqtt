# DM-aiomqtt

## Urls

* [PyPI](https://pypi.org/project/dm-aiomqtt)
* [GitHub](https://github.com/DIMKA4621/dm-aiomqtt)

## Usage

### Run in Windows

_If you run async code in **Windows**, set correct selector_

```python
import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
```

### One connection for all actions

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
    await mqtt_client.add_topic_handler("test", test_topic_handler)

    # start connection
    await mqtt_client.connect()

    # publish
    mqtt_client.publish("test", payload="Hello World!", logging=True)

    # other code (for example, endless waiting)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

### Temporary connection for actions

_The `apublish` method is used here to **wait** for messages to be sent_

```python
from dm_aiomqtt import DMAioMqttClient
import asyncio


async def main():
    # create client
    mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

    # create callback
    async def callback(apublish: DMAioMqttClient.apublish):
        await apublish("test/1", payload="Hello World!1", qos=2, logging=True)
        # other code
        await apublish("test/2", payload="Hello World!2", qos=2, logging=True)

    # execute callback in temporary connection
    await mqtt_client.temp_connect(callback)

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
    def info(self, message):
       pass

    def warning(self, message):
       print(message)

    def error(self, message):
       print(message)


# set up custom logger for all clients
DMAioMqttClient.set_logger(MyLogger())
```
