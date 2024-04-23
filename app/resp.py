"""
Redis Serialization Protocol (RESP) Implementation

Reference:
https://redis.io/docs/latest/develop/reference/protocol-spec/
"""


import asyncio
import logging
import time

from dataclasses import dataclass

from . import config


# asyncio uses the logging module and all logging is performed via the "asyncio" logger.
# Reference: https://docs.python.org/3/library/asyncio-dev.html#logging
logger = logging.getLogger('asyncio')


@dataclass(frozen=True)
class DataType:
    ARRAY         = b'*'
    BULK_STRING   = b'$'
    SIMPLE_STRING = b'+' 
    SIMPLE_ERROR  = b'-'


@dataclass
class Constant:
    NULL_BULK_STRING = b'$-1\r\n'
    TERMINATOR       = b'\r\n'


@dataclass
class Command:
    PING = 'ping'
    ECHO = 'echo'
    SET  = 'set'
    GET  = 'get'
    INFO = 'info'


store = {}


async def parse(reader: asyncio.StreamReader):
    """
    Parse commands (`bytes`) from socket stream using `reader` and return as a `list`.
    
    Clients send commands to a Redis server as an array of bulk strings. 
    The first (and sometimes also the second) bulk string in the array is the command's name.
    
    Example: *2\r\n$4\r\necho\r\n$3\r\nhello\r\n
    """

    try:
        _ = await reader.read(1)
        if _ != b'*': # not an array
            logger.error(f'Expected {DataType.ARRAY}, got {_}')
            return []
        
        num_commands = int(await reader.readuntil(Constant.TERMINATOR))
        # note: even though read.readuntil() returns bytes along with the terminator,
        # int() is able to handle bytes and surrounding whitespaces. 
        # note: '\r' and '\n' are counted as whitespaces.

        commands = []
        
        while len(commands) < num_commands:
    
            datatype = await reader.read(1)

            if datatype == DataType.BULK_STRING:
                length = int(await reader.readuntil(Constant.TERMINATOR))
                data   = await reader.read(length)

                _ = await reader.read(2) 
                # terminator not found after `length` bytes
                if _ != Constant.TERMINATOR:
                    logger.error(f"Expected {Constant.TERMINATOR}, got {_}")
                    return []

                commands.append(data.decode())

            else:
                logger.error(f'Expected {DataType.BULK_STRING}, got {datatype}')
                return []

        return commands

    except Exception as e:
        logger.exception(e)
        return []


async def encode(datatype: DataType, data: str):
    """
    Encode data as per RESP specifications
    """
    
    if datatype in (DataType.SIMPLE_STRING, DataType.SIMPLE_ERROR):
        return b'\r\n'.join([datatype + data.encode(), b''])

    if datatype == DataType.BULK_STRING:
        data   = data.encode()
        length = len(data)
        return b'\r\n'.join([datatype + str(length).encode(), data, b''])
    
    return b''

async def execute(commands: list[str]):
    """
    Execute commands and return the result.
    """

    if not commands:
        return await encode(DataType.SIMPLE_ERROR, 'Invalid command')
    
    if commands[0].lower() == Command.PING:
        return await encode(DataType.SIMPLE_STRING, 'PONG')
    
    if commands[0].lower() == Command.ECHO:        
        return await encode(DataType.SIMPLE_STRING, commands[1])

    if commands[0].lower() == Command.SET:
        key   = commands[1]
        value = commands[2]

        store[key] = {'value': value, 'px': None}

        if len(commands) == 5:
            px = int(commands[4])
            store[key]['px'] = time.time() * 1000 + px

        return await encode(DataType.SIMPLE_STRING, 'OK')

    if commands[0].lower() == Command.GET:
        key = commands[1]

        if key in store:
            timestamp_ms = time.time() * 1000
            if not store[key]['px'] or timestamp_ms < store[key]['px']:
                return await encode(DataType.BULK_STRING, store[key]['value'])
                
            store.pop(key)

        return Constant.NULL_BULK_STRING

    if commands[0].lower() == Command.INFO:
        section = commands[1]
        print(section)
        section_config = config.config[section]
        data = '\n'.join([f'{key}:{value}' for key, value in section_config.items()]).encode()
        print(data)
        return await encode(DataType.BULK_STRING, data)
