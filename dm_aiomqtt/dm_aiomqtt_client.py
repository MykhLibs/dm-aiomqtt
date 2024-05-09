from dm_logger import DMLogger
from typing import Union, Callable, Coroutine, Literal, List, Optional
import asyncio
import aiomqtt
import json
import ssl
import re
import os
from .message_db import MessageDB


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

    __protocol_versions = {3: aiomqtt.ProtocolVersion.V31,
                           4: aiomqtt.ProtocolVersion.V311,
                           5: aiomqtt.ProtocolVersion.V5}
    __default_version = 4
    __logger = None

    def __init__(
        self,
        host: str,
        port: int,
        username: str = "",
        password: str = "",
        version: int = 4,  # 3 => v3.1;  4 => v3.1.1;  5 => v5
        ca_crt: str = "",
        client_crt: str = "",
        client_key: str = "",
        keepalive: int = 5,
        identifier: str = None,
        clean_session: Optional[bool] = None,  # not supported for v5 (for v5 by default => CLEAN_START=FIRST_ONLY)
        resend_not_success_messages: bool = False
    ) -> None:
        if self.__logger is None:
            self.__logger = DMLogger(f"DMAioMqttClient-{host}:{port}")

        self.__mqtt_config = {
            "hostname": host,
            "port": port,
            "keepalive": keepalive,
            "clean_session": clean_session
        }
        if version not in self.__protocol_versions:
            default_protocol = self.__protocol_versions[self.__default_version]
            self.__mqtt_config["protocol"] = default_protocol
            self.__logger.warning(f"Invalid protocol version: '{version}'! "
                                  f"Used default version: {self.__default_version} ({default_protocol})")
        else:
            self.__mqtt_config["protocol"] = self.__protocol_versions[version]

        if self.__mqtt_config["protocol"] == aiomqtt.ProtocolVersion.V5:
            self.__mqtt_config["clean_session"] = None

        if identifier:
            self.__mqtt_config["identifier"] = identifier
        if username or password:
            self.__mqtt_config["username"] = username
            self.__mqtt_config["password"] = password
        self.__mqtt_config["tls_context"] = self.__get_tls_context(ca_crt, client_crt, client_key)

        self.__subscribes = {}
        self.__pattern_subscribes = {}
        self.__resend_ns_msg = resend_not_success_messages
        self.__message_db = MessageDB()

        self.__client: aiomqtt.Client = aiomqtt.Client(**self.__mqtt_config)
        self.__connected_event = asyncio.Event()

    async def start(self) -> None:
        _ = asyncio.create_task(self.__connect_loop())
        await self.__connected_event.wait()

    async def __connect_loop(self) -> None:
        while True:
            try:
                async with aiomqtt.Client(**self.__mqtt_config) as self.__client:
                    self.__logger.info("Connected!")
                    self.__connected_event.set()

                    if self.__resend_ns_msg:
                        await self.__resend_not_success_messages()
                    await self.__subscribe()
                    await self.__listen()
            except Exception as e:
                if self.__connected_event.is_set():
                    self.__logger.error(f"Error: {e}")
                self.__connected_event.clear()
                self.__connecting = False

    async def __listen(self) -> None:
        async for message in self.__client.messages:
            topic = message.topic.value
            payload = message.payload.decode('utf-8')

            callbacks = self.__get_callbacks_from_pattern_subscribes(topic)
            topic_params = self.__subscribes.get(topic)
            if isinstance(topic_params, dict):
                callbacks.append(topic_params["cb"])

            for callback in callbacks:
                if isinstance(callback, Callable):
                    _ = asyncio.create_task(callback(self.publish, topic, payload))
                else:
                    self.__logger.error(f"Callback is not a Callable object: {type(callback)}, {topic=}")

    def add_topic_handler(self, topic: str, callback: _SUBSCRIBE_CALLBACK_TYPE, qos: _QOS_TYPE = 0) -> None:
        """
        callback EXAMPLE:
            async def test_topic_handler(publish: DMAioMqttClient.publish, topic: str, payload: str) -> None:
               print(f"Received message from {topic}: {payload}")
               publish("test/success", payload=True)
        """
        new_item = {"cb": callback, "qos": qos}
        self.__subscribes[topic] = new_item

        if re.search(r"[+#$]", topic):
            self.__pattern_subscribes[topic] = new_item

    def publish(
        self,
        topic: str,
        payload: Union[str, int, float, dict, list, bool, None],
        qos: _QOS_TYPE = 0,
        *,
        payload_to_json: Union[bool, Literal["auto"]] = "auto",
        sent_logging: bool = False,
        error_logging: bool = False,
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

        async def cb(payload):
            if payload_to_json is True or (payload_to_json == "auto" and type(payload) not in (str, int, float)):
                payload = json.dumps(payload, ensure_ascii=False, separators=(',', ':'))
            try:
                await self.__client.publish(topic, payload, qos)
            except Exception as e:
                if self.__resend_ns_msg:
                    await self.__message_db.insert([topic, payload, qos])
                if error_logging:
                    self.__logger.warning(f"Publish not sent: {e}")
            else:
                if sent_logging:
                    self.__logger.debug(f"Published message to '{topic}' topic ({qos=}): {payload}")

        _ = asyncio.create_task(cb(payload))

    async def __resend_not_success_messages(self) -> None:
        messages = await self.__message_db.get_all()
        for topic, payload, qos in messages:
            self.publish(topic, payload, qos)

    def __get_callbacks_from_pattern_subscribes(self, current_topic: str) -> List[Callable]:
        callbacks = []
        for topic, params in self.__pattern_subscribes.items():
            if topic[0] == "$":
                topic = "/".join(topic.split("/")[2:])
            pattern = topic.replace("+", "[^/]+?")
            pattern = pattern.replace("/#", "(/.+)*")
            if re.search(pattern, current_topic):
                callbacks.append(params["cb"])
        return callbacks

    async def __subscribe(self) -> None:
        for topic, params in self.__subscribes.items():
            _, qos = params.values()
            await self.__client.subscribe(topic, qos)
            self.__logger.debug(f"Subscribe to '{topic}' topic ({qos=})")

    def __get_tls_context(self, ca_crt: str, client_crt: str, client_key: str) -> Optional[ssl.SSLContext]:
        if not ca_crt and (client_crt or client_key):
            self.__logger.error("ca_crt file is not specified!")
            return

        if ca_crt:
            if not os.path.exists(ca_crt):
                self.__logger.error(f"'ca_crt' file '{ca_crt}' file not found!")
                return
            tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_crt)

            if client_crt and client_key:
                if not os.path.exists(client_crt):
                    self.__logger.error(f"'client_crt' file '{client_crt}' file not found!")
                elif not os.path.exists(client_key):
                    self.__logger.error(f"'client_key' file '{client_key}' file not found!")
                else:
                    tls_context.load_cert_chain(certfile=client_crt, keyfile=client_key)
            elif client_crt or client_key:
                self.__logger.error("'client_crt' or 'client_key' file is not specified!")

            return tls_context

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
