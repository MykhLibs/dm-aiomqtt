from dm_logger import DMLogger
from datetime import datetime
from typing import List
import aiofiles
import json
import os

__all__ = ["MessageDB"]


class AioFileManager:
    _logger = DMLogger("AioFileManager")

    @staticmethod
    async def write_jsonl(filename: str, obj: list) -> None:
        content = json.dumps(obj, ensure_ascii=False)
        async with aiofiles.open(filename, "a", encoding="utf-8") as file:
            await file.write(f"{content}\n")

    @classmethod
    async def read_jsonl(cls, filename: str) -> List[list]:
        if os.path.exists(filename):
            data = []
            async with aiofiles.open(filename, "r", encoding="utf-8") as file:
                for line in await file.readlines():
                    try:
                        data.append(json.loads(line))
                    except:
                        pass
            return data
        return []

    @staticmethod
    async def clear_file(filename: str) -> None:
        async with aiofiles.open(filename, "w"):
            pass


class MessageDB:
    def __init__(self, filename: str = ".mqtt_msg_db.jsonl") -> None:
        self._filename = filename
        self._file_mng = AioFileManager()

    async def insert(self, record: list) -> None:
        await self._file_mng.write_jsonl(self._filename, record)

    async def get_all(self) -> List[list]:
        messages = await self._file_mng.read_jsonl(self._filename)
        await self._file_mng.clear_file(self._filename)
        return messages
