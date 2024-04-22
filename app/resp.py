"""
Redis Serialization Protocol (RESP) Implementation

Reference:
https://redis.io/docs/latest/develop/reference/protocol-spec/
"""


import asyncio
import logging

from dataclasses import dataclass


# asyncio uses the logging module and all logging is performed via the "asyncio" logger.
# Reference: https://docs.python.org/3/library/asyncio-dev.html#logging
logger = logging.getLogger('asyncio')


@dataclass(frozen=True)
class DataType:
    ARRAY         = b'*'
    BULK_STRING   = b'$'
    SIMPLE_STRING = b'+' 
    SIMPLE_ERROR  = b'-'


async def parse(reader: asyncio.StreamReader):
    """
    Parse commands (`bytes`) from socket stream using `reader` and return as a `list`.
    
    Clients send commands to a Redis server as an array of bulk strings. 
    The first (and sometimes also the second) bulk string in the array is the command's name.
    
    Example: *2\r\n$4\r\necho\r\n$3\r\nhello\r\n
    """

    terminator = b'\r\n'

    try:
        _ = await reader.read(1)
        if _ != b'*': # not an array
            logger.error(f'Expected {DataType.ARRAY}, got {_}')
            return []
        
        num_commands = int(await reader.readuntil(terminator))
        # note: even though read.readuntil() returns bytes along with the terminator,
        # int() is able to handle bytes and surrounding whitespaces. 
        # note: '\r' and '\n' are counted as whitespaces.

        commands = []
        
        while True:        
            datatype = await reader.read(1)
            if not datatype:
                if len(commands) != num_commands:
                    logger.error(f'Expected {num_commands} commands, got {len(commands)}')
                    return []
                break

            if datatype == DataType.BULK_STRING:
                length = int(await reader.readuntil(terminator))
                data   = await reader.read(length)

                _ = await reader.read(2) 
                # terminator not found after `length` bytes
                if _ != '\r\n':
                    logger.error(f"Expected {terminator}, got {_}")
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
    data   = data.encode()
    length = len(data)
    return b'\r\n'.join([datatype + str(length).encode(), data, b''])


async def execute(commands: list[str]):
    """
    Execute commands and return the result.
    """

    if not commands:
        return await encode(DataType.SIMPLE_ERROR, 'Invalid command')
    
    if commands[0] == 'ping':
        return await encode(DataType.SIMPLE_STRING, 'PONG')
    
    if commands[0] == 'echo':        
        return await encode(DataType.SIMPLE_STRING, commands[1])
