from dm_logger import DMLogger
from typing import Union, Callable, Coroutine, Literal, Optional, Dict
import asyncio
import aiomqtt
import json


class DMAioMqttClient:
    """
    See usage examples here:
        https://pypi.org/project/dm-aiomqtt
        https://github.com/DIMKA4621/dm-aiomqtt
    """
    _SUBSCRIBE_CALLBACK_TYPE = Callable[["DMAioMqttClient.publish", str, str], Coroutine]
    _TEMP_CALLBACK_TYPE = Callable[["DMAioMqttClient.apublish"], Coroutine]
    _LOG_FN_TYPE = Callable[[str], None]
    _QOS_TYPE = Literal[0, 1, 2]

    """If reconnect_interval=None, reconnection will not occur"""
    reconnect_interval: Optional[int] = 10
    __logger = None

    def __init__(self, host: str, port: int, username: str = "", password: str = "") -> None:
        if self.__logger is None:
            self.__logger = DMLogger(f"DMAioMqttClient-{host}:{port}")

        self.__connection_config = {
            "hostname": host,
            "port": port,
        }
        if username or password:
            self.__connection_config["username"] = username
            self.__connection_config["password"] = password

        self.__subscribes = {}
        self.__client = None

    async def add_topic_handler(self, topic: str, callback: _SUBSCRIBE_CALLBACK_TYPE, qos: _QOS_TYPE = 0) -> None:
        """
        callback EXAMPLE:
            async def test_topic_handler(publish: DMAioMqttClient.publish, topic: str, payload: str) -> None:
               print(f"Received message from {topic}: {payload}")
               publish("test/success", payload=True)
        """
        new_item = {"cb": callback, "qos": qos}
        self.__subscribes[topic] = new_item
        if self.__client is not None:
            await self.__subscribe({topic: new_item})

    async def connect(self) -> None:
        if self.__client is not None:
            self.__logger.warning("Client is already connected!")
            return

        async def coroutine() -> None:
            self.__client = aiomqtt.Client(**self.__connection_config)
            await self.__client.__aenter__()
            self.__logger.info("Connected to mqtt broker!")
            await self.__subscribe(self.__subscribes)
            _ = asyncio.create_task(self.__listen())

        await self.__handle_connection_errors(coroutine())

    async def temp_connect(self, callback: _TEMP_CALLBACK_TYPE) -> None:
        """
        callback EXAMPLE:
            async def temp_callback(apublish: DMAioMqttClient.apublish) -> None:
                await apublish("test/1", payload="Hello World!1")
                await apublish("test/2", payload="Hello World!2")
        """
        if not isinstance(callback, Callable):
            self.__logger.error(f"Callback is not a Callable object: {type(callback)}")
            return
        if self.__client is not None:
            self.__logger.warning("Client is already connected!")
            return

        async def coroutine() -> None:
            self.__client = aiomqtt.Client(**self.__connection_config)
            await self.__client.__aenter__()
            self.__logger.info("Connected to mqtt broker!")
            await callback(self.apublish)
            await self.disconnect()

        await self.__handle_connection_errors(coroutine())

    async def disconnect(self) -> None:
        if self.__client is None:
            self.__logger.warning("Client is not connected!")
            return
        await self.__client.__aexit__(None, None, None)
        self.__logger.info("Disconnected!")
        self.__client = None

    async def __subscribe(self, subscribes: Dict[str, Dict[str, Union[_SUBSCRIBE_CALLBACK_TYPE, _QOS_TYPE]]]) -> None:
        for topic, params in subscribes.items():
            _, qos = params.values()
            await self.__client.subscribe(topic, qos)
            self.__logger.info(f"Subscribe to '{topic}' topic ({qos=})")

    async def __listen(self) -> None:
        async def coroutine() -> None:
            async for message in self.__client.messages:
                topic = message.topic.value
                payload = message.payload.decode('utf-8')

                topic_params = self.__subscribes.get(topic)
                if isinstance(topic_params, dict):
                    callback = topic_params["cb"]
                    if isinstance(callback, Callable):
                        _ = asyncio.create_task(callback(self.publish, topic, payload))
                    else:
                        self.__logger.error(f"Callback is not a Callable object: {type(callback)}, {topic=}")

        await self.__handle_connection_errors(coroutine())

    def publish(
        self,
        topic: str,
        payload: Union[str, int, float, dict, list, bool, None],
        qos: _QOS_TYPE = 0,
        *,
        payload_to_json: Union[bool, Literal["auto"]] = "auto",
        logging: bool = False
    ) -> None:
        task = self.apublish(topic, payload, qos, payload_to_json=payload_to_json, logging=logging)
        _ = asyncio.create_task(task)

    async def apublish(
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
        if self.__client is None:
            self.__logger.warning("Client is not connected!")
            return
        if payload_to_json is True or (payload_to_json == "auto" and type(payload) not in (str, int, float)):
            payload = json.dumps(payload, ensure_ascii=False)

        try:
            await self.__client.publish(topic, payload, qos)
        except Exception as e:
            self.__logger.error(f"Publish error: {e}")
        else:
            if logging:
                self.__logger.info(f"Published message to '{topic}' topic ({qos=}): {payload}")

    async def __handle_connection_errors(self, coroutine: Coroutine):
        try:
            await coroutine
        except aiomqtt.exceptions.MqttError as e:
            err_msg = f"Connection error: {e}."
            if self.reconnect_interval is None:
                self.__logger.error(err_msg)
                self.__client = None
            else:
                self.__logger.error(f"{err_msg}\nReconnecting in {self.reconnect_interval} seconds...")
                self.__client = None
                await asyncio.sleep(self.reconnect_interval)
                await self.connect()
        except Exception as e:
            self.__logger.error(f"Error: {e}")
            self.__client = None

    @classmethod
    def set_logger(cls, logger) -> None:
        if (hasattr(logger, "info") and isinstance(logger.info, Callable) and
            hasattr(logger, "warning") and isinstance(logger.warning, Callable) and
            hasattr(logger, "error") and isinstance(logger.error, Callable)
        ):
            cls.__logger = logger
        else:
            print("Invalid logger")
