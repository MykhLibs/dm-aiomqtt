from dm_logger import DMLogger
from typing import Union, Callable, Coroutine, Literal, List
import asyncio
import aiomqtt
import json
import uuid
import re


class DMAioMqttClient:
    """
    See usage examples here:
        https://pypi.org/project/dm-aiomqtt
        https://github.com/DIMKA4621/dm-aiomqtt
    """
    _SUBSCRIBE_CALLBACK_TYPE = Callable[["DMAioMqttClient.publish", str, str], Coroutine]
    _TEMP_CALLBACK_TYPE = Callable[["DMAioMqttClient.publish"], Coroutine]
    _LOG_FN_TYPE = Callable[[str], None]
    _QOS_TYPE = Literal[0, 1, 2]

    __logger = None

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
        ping_interval_s: int = 5,
        clean_session: bool = True
    ) -> None:
        if self.__logger is None:
            self.__logger = DMLogger(f"DMAioMqttClient-{host}:{port}")

        self.__ping_interval_s = ping_interval_s if ping_interval_s > 1 else 1

        self.__mqtt_config = {
            "hostname": host,
            "port": port,
            "keepalive": self.__ping_interval_s * 3,
            "clean_session": clean_session,
            "identifier": str(uuid.uuid4())
        }
        if username or password:
            self.__mqtt_config["username"] = username
            self.__mqtt_config["password"] = password

        self.__reconnect_timeout = self.__ping_interval_s * 2 + 1
        self.__reconnect_timer_task = None
        self.__is_reconnect = False
        self.__subscribes = {}
        self.__ping_topic = f"ping/{self.__mqtt_config['identifier']}"
        self.add_topic_handler(self.__ping_topic, self.__reset_reconnect_timer_task)
        self.__client: aiomqtt.Client = None

    async def start(self) -> None:
        await self.__connect_loop()
        _ = asyncio.create_task(self.__ping_loop())

    async def __connect(self) -> None:
        self.__client = aiomqtt.Client(**self.__mqtt_config)
        await self.__client.__aenter__()
        self.__logger.info("Connected!")

    async def __connect_loop(self) -> None:
        while True:
            try:
                await self.__connect()
                self.__is_reconnect = False
                self.__reconnect_timer_task = asyncio.create_task(self.__reconnect_timer())
                await self.__subscribe()
                _ = asyncio.create_task(self.__listen())
                return
            except Exception as e:
                self.__logger.error(f"Connection error: {e}.\nReconnecting...")
                await asyncio.sleep(self.__ping_interval_s)

    async def __disconnect(self) -> None:
        try:
            await self.__client.__aexit__(None, None, None)
        except:
            pass

    async def __reconnect(self) -> None:
        self.__reconnect_timer_task.cancel()
        if self.__is_reconnect:
            return
        self.__is_reconnect = True
        await self.__disconnect()
        await self.__connect_loop()

    async def __reset_reconnect_timer_task(self, *args, **kwargs) -> None:
        self.__reconnect_timer_task.cancel()
        self.__reconnect_timer_task = asyncio.create_task(self.__reconnect_timer())

    async def __reconnect_timer(self) -> None:
        await asyncio.sleep(self.__reconnect_timeout)
        await self.__reconnect()

    async def __ping_loop(self) -> None:
        while True:
            await self.publish(self.__ping_topic, 1)
            await asyncio.sleep(self.__ping_interval_s)

    async def __listen(self) -> None:
        try:
            async for message in self.__client.messages:
                topic = message.topic.value
                payload = message.payload.decode('utf-8')

                callbacks = self.__get_topics_from_pattern(topic)
                topic_params = self.__subscribes.get(topic)
                if isinstance(topic_params, dict):
                    callbacks.append(topic_params["cb"])

                for callback in callbacks:
                    if isinstance(callback, Callable):
                        _ = asyncio.create_task(callback(self.publish, topic, payload))
                    else:
                        self.__logger.error(f"Callback is not a Callable object: {type(callback)}, {topic=}")
        except Exception as e:
            self.__logger.error(f"Connection error: {e}")
            await self.__reconnect()

    def __get_topics_from_pattern(self, current_topic: str) -> List[Callable]:
        callbacks = []
        for topic, params in self.__subscribes.items():
            pattern = None
            if "+" in topic:
                pattern = topic.replace("+", "[^/]+?")
            if "#" in topic:
                if pattern:
                    pattern = pattern.replace("/#", "(/.+)*")
                else:
                    pattern = topic.replace("/#", "(/.+)*")
            if pattern is not None and re.search(pattern, current_topic):
                callbacks.append(params["cb"])
        return callbacks

    async def publish(
        self,
        topic: str,
        payload: Union[str, int, float, dict, list, bool, None],
        qos: _QOS_TYPE = 0,
        *,
        payload_to_json: Union[bool, Literal["auto"]] = "auto",
        sent_logging: bool = False,
        warn_logging: bool = False,
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
        try:
            await self.__client.publish(topic, payload, qos)
        except Exception as e:
            if warn_logging:
                self.__logger.warning(f"Publish not sent: {e}")
        else:
            if sent_logging:
                self.__logger.debug(f"Published message to '{topic}' topic ({qos=}): {payload}")

    def add_topic_handler(self, topic: str, callback: _SUBSCRIBE_CALLBACK_TYPE, qos: _QOS_TYPE = 0) -> None:
        """
        callback EXAMPLE:
            async def test_topic_handler(publish: DMAioMqttClient.publish, topic: str, payload: str) -> None:
               print(f"Received message from {topic}: {payload}")
               await publish("test/success", payload=True)
        """
        new_item = {"cb": callback, "qos": qos}
        self.__subscribes[topic] = new_item

    async def __subscribe(self) -> None:
        for topic, params in self.__subscribes.items():
            _, qos = params.values()
            await self.__client.subscribe(topic, qos)
            self.__logger.debug(f"Subscribe to '{topic}' topic ({qos=})")

    @classmethod
    def set_logger(cls, logger) -> None:
        if (hasattr(logger, "debug") and isinstance(logger.debug, Callable) and
            hasattr(logger, "info") and isinstance(logger.info, Callable) and
            hasattr(logger, "warning") and isinstance(logger.warning, Callable) and
            hasattr(logger, "error") and isinstance(logger.error, Callable)
        ):
            cls.__logger = logger
        else:
            print("Invalid logger")
