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

### Listen

```python
from dm_aiomqtt import DMAioMqttClient
import asyncio


# create handler for 'test' topic
async def test_handler(client: DMAioMqttClient, topic: str, payload: str) -> None:
   print(f"Received message from {topic}: {payload}")
   await client.publish("test/success", payload=True)


async def main():
   # create client
   mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

   # add handler for 'test' topic
   mqtt_client.add_handler("test", test_handler)

   # start listening (this operation is block async thread)
   await mqtt_client.listen()


if __name__ == "__main__":
   asyncio.run(main())
```

### Publish

* Connection only when publishing

   ```python
   from dm_aiomqtt import DMAioMqttClient
   import asyncio


   async def main():
       # create client
       mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

       # publish json message
       await mqtt_client.publish("test", payload={"key": "value"})

       # other code

       # publish new json message
       await mqtt_client.publish("test2", payload={"key2": "value2"})


   if __name__ == "__main__":
       asyncio.run(main())
   ```

* One connection for all publishing

   ```python
   from dm_aiomqtt import DMAioMqttClient
   import asyncio


   async def main(mqtt_client: DMAioMqttClient):
       # publish json message
       await mqtt_client.publish("test", payload={"key": "value"})

       # other code

       # publish new json message
       await mqtt_client.publish("test2", payload={"key2": "value2"})


   async def start():
       # create client
       mqtt_client = DMAioMqttClient("localhost", 1883, "username", "password")

       # create task for listening (this operation is block async thread)
       listen_task = asyncio.create_task(mqtt_client.listen(reconnect_interval=5))

       # create task to execute other code with an active connection
       main_task = asyncio.create_task(main(mqtt_client))

       # run async tasks in parallel
       await asyncio.gather(listen_task, main_task)


   if __name__ == "__main__":
       asyncio.run(start())
   ```
