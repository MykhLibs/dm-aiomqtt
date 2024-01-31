from dm_logger import DMLogger
from typing import Union, Callable, Coroutine, Literal
import asyncio
import aiomqtt
import json


class DMAioMqttClient:
    """
    See usage examples here:
        https://pypi.org/project/dm-aiomqtt
        https://github.com/DIMKA4621/dm-aiomqtt
    """
    _CALLBACK_TYPE = Callable[["DMAioMqttClient", str, str], Coroutine]
    _QOS_TYPE = Literal[0, 1, 2]

    __logger = DMLogger("DMAioMqttClient")
    __instances = {}

    def __new__(cls, host: str, port: int, username: str = None, password: str = None, *args, **kwargs):
        key = (host, port, username, password)
        if key not in cls.__instances:
            cls.__instances[key] = super().__new__(cls)
        return cls.__instances[key]

    def __init__(self, host: str, port: int, username: str = None, password: str = None) -> None:
        self.__connection_config = {
            "hostname": host,
            "port": port,
            "username": username,
            "password": password,
        }
        self._info = f"{host}:{port}"
        self.__subscribes = {}
        self.__client = None

    def add_handler(self, topic: str, callback: _CALLBACK_TYPE, qos: _QOS_TYPE = 0) -> None:
        """
        callback EXAMPLE:
            async def test_handler(client: DMAioMqttClient, topic: str, payload: str) -> None:
               print(f"Received message from {topic}: {payload}")
               await client.publish("test/success", payload=True)
        """
        self.__subscribes[topic] = {"cb": callback, "qos": qos}

    async def __execute(self, callback: Callable, reconnect: bool = False, interval: int = 10) -> None:
        try:
            if self.__client is None:
                async with aiomqtt.Client(**self.__connection_config) as self.__client:
                    await callback()
                self.__client = None
            else:
                await callback()
        except Exception as e:
            err_msg = f"Connection error: {e}."
            if reconnect:
                self.__logger.error(f"{err_msg}\nReconnecting in {interval} seconds...", info=self._info)
                self.__client = None
                await asyncio.sleep(interval)
                await self.__execute(callback, reconnect, interval)
            else:
                self.__logger.error(f"{err_msg}", info=self._info)

    async def listen(self, *, reconnect_interval: int = 10) -> None:
        async def callback() -> None:
            self.__logger.info("Connected to mqtt broker!", info=self._info)

            for topic, params in self.__subscribes.items():
                _, qos = params.values()
                await self.__client.subscribe(topic, qos)
                self.__logger.debug(f"Subscribe to '{topic}' topic ({qos=})", info=self._info)

            await self.__message_handler()

        await self.__execute(callback, reconnect=True, interval=reconnect_interval)

    async def __message_handler(self) -> None:
        self.__logger.info("Listening...", info=self._info)
        async for message in self.__client.messages:
            topic = message.topic.value
            payload = message.payload.decode('utf-8')

            topic_params = self.__subscribes.get(topic)
            if isinstance(topic_params, dict):
                callback = topic_params["cb"]
                if isinstance(callback, Callable):
                    await callback(self, topic, payload)
                else:
                    self.__logger.error(f"Callback is not a Callable object: {type(callback)}, {topic=}",
                                        info=self._info)

    async def publish(
        self,
        topic: str,
        payload: Union[str, int, float, dict, list, bool, None],
        qos: _QOS_TYPE = 0,
        *,
        payload_to_json: Union[bool, Literal["auto"]] = "auto",
        logging: bool = False
    ) -> None:
        """
        payload_to_json (bool, "auto"):
            - "auto":
                will be converted all payload types expect: str, int, float
            - True:
                will be converted all payload types
            - False:
                will not be converted
        """
        if payload_to_json is True or (payload_to_json == "auto" and type(payload) not in (str, int, float)):
            payload = json.dumps(payload, ensure_ascii=False)

        async def callback() -> None:
            await self.__client.publish(topic, payload, qos)

        await self.__execute(callback)
        if logging:
            self.__logger.debug(f"Published message to '{topic}' topic ({qos=}): {payload}", info=self._info)
