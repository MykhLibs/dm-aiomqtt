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
    _LOG_FN_TYPE = Callable[[str], None]
    _QOS_TYPE = Literal[0, 1, 2]

    __info_fn: _LOG_FN_TYPE = None
    __error_fn: _LOG_FN_TYPE = None
    __instances: dict = {}

    def __new__(cls, host: str, port: int, username: str = "", password: str = "", *args, **kwargs):
        key = (host, port, username, password)
        if key not in cls.__instances:
            cls.__instances[key] = super().__new__(cls)
        return cls.__instances[key]

    def __init__(self, host: str, port: int, username: str = "", password: str = "") -> None:
        if self.__info_fn is None or self.__error_fn is None:
            logger = DMLogger(f"DMAioMqttClient-{host}:{port}")

            if self.__info_fn is None:
                self.__info_fn = logger.info
            if self.__error_fn is None:
                self.__error_fn = logger.error

        self.__connection_config = {
            "hostname": host,
            "port": port,
        }
        if username or password:
            self.__connection_config["username"] = username
            self.__connection_config["password"] = password

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
        except aiomqtt.exceptions.MqttError as e:
            err_msg = f"Connection error: {e}."
            if reconnect:
                self.__error(f"{err_msg}\nReconnecting in {interval} seconds...")
                self.__client = None
                await asyncio.sleep(interval)
                await self.__execute(callback, reconnect, interval)
            else:
                self.__error(f"{err_msg}")

    async def listen(self, *, reconnect_interval: int = 10) -> None:
        async def callback() -> None:
            self.__info("Connected to mqtt broker!")

            for topic, params in self.__subscribes.items():
                _, qos = params.values()
                await self.__client.subscribe(topic, qos)
                self.__info(f"Subscribe to '{topic}' topic ({qos=})")

            await self.__message_handler()

        await self.__execute(callback, reconnect=True, interval=reconnect_interval)

    async def __message_handler(self) -> None:
        self.__info("Listening...")
        async for message in self.__client.messages:
            topic = message.topic.value
            payload = message.payload.decode('utf-8')

            topic_params = self.__subscribes.get(topic)
            if isinstance(topic_params, dict):
                callback = topic_params["cb"]
                if isinstance(callback, Callable):
                    await callback(self, topic, payload)
                else:
                    self.__error(f"Callback is not a Callable object: {type(callback)}, {topic=}")

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
            self.__info(f"Published message to '{topic}' topic ({qos=}): {payload}")

    @classmethod
    def set_log_functions(cls, *, info_logs: _LOG_FN_TYPE = None, err_logs: _LOG_FN_TYPE = None) -> None:
        if isinstance(info_logs, Callable):
            cls.__info_fn = info_logs
        if isinstance(err_logs, Callable):
            cls.__error_fn = err_logs

    def __info(self, message: str) -> None:
        if self.__info_fn is not None:
            self.__info_fn(message)

    def __error(self, message: str) -> None:
        if self.__error_fn is not None:
            self.__error_fn(message)
