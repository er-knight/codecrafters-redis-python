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
    EMPTY_BYTE       = b''
    SPACE_BYTE       = b' '
    PONG             = b'PONG'
    OK               = b'OK'
    INVALID_COMMAND  = b'Invalid Command'
    FULLRESYNC       = b'FULLRESYNC'
    

@dataclass
class Command:
    PING     = 'ping'
    ECHO     = 'echo'
    SET      = 'set'
    GET      = 'get'
    INFO     = 'info'
    REPLCONF = 'replconf'
    PSYNC    = 'psync'


store = {}
rdb_state = bytes.fromhex('524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2')


async def parse_commands(reader: asyncio.StreamReader):
    """
    Parse commands (`bytes`) from socket stream using `reader` and return as a `list`.
    
    Clients send commands to a Redis server as an array of bulk strings. 
    The first (and sometimes also the second) bulk string in the array is the command's name.
    
    Example: *2\r\n$4\r\necho\r\n$3\r\nhello\r\n
    """

    try:
        print(f'reading from {reader}')
        _ = await reader.read(1)
        if _ != DataType.ARRAY:
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


async def parse_response(reader: asyncio.StreamReader):
    """
    Parse response of executed command
    """

    async def parse_datatype():
        return await reader.read(1)
    
    async def parse_bulk_string():
        length = int(await reader.readuntil(Constant.TERMINATOR))
        data   = await reader.read(length)

        _ = await reader.read(2) 
        # terminator not found after `length` bytes
        if _ != Constant.TERMINATOR:
            logger.error(f"Expected {Constant.TERMINATOR}, got {_}")
            return Constant.EMPTY_BYTE

        return data

    async def parse_simple_string():
        data = await reader.readuntil(Constant.TERMINATOR)
        return data.strip(Constant.TERMINATOR)

    async def parse_simple_error():
        data = await reader.readuntil(Constant.TERMINATOR)
        return data.strip(Constant.TERMINATOR)
    
    async def parse_array():
        num_elements = int(await reader.readuntil(Constant.TERMINATOR))    
        elements = []

        while num_elements > 0:
        
            datatype = await parse_datatype()
        
            if datatype == DataType.BULK_STRING:
                element = await parse_bulk_string()
            elif datatype == DataType.SIMPLE_STRING:
                element = await parse_simple_string()
            elif datatype == DataType.SIMPLE_ERROR:
                element = await parse_simple_error()
            
            elements.append(element)
            num_elements -= 1

        return elements

    datatype = await parse_datatype()

    if datatype == DataType.ARRAY:
        return await parse_array()
    elif datatype == DataType.BULK_STRING:
        return await parse_bulk_string()
    elif datatype == DataType.SIMPLE_STRING:
        return await parse_simple_string()
    elif datatype == DataType.SIMPLE_ERROR:
        return await parse_simple_error()
            

async def encode(datatype: DataType, data: bytes | list[bytes]):
    """
    Encode data as per RESP specifications
    """
    
    if datatype in (DataType.SIMPLE_STRING, DataType.SIMPLE_ERROR):
        return Constant.TERMINATOR.join([datatype + data, Constant.EMPTY_BYTE])

    if datatype == DataType.BULK_STRING:
        length = len(data)
        return Constant.TERMINATOR.join([datatype + str(length).encode(), data, Constant.EMPTY_BYTE])
    
    if datatype == DataType.ARRAY:
        num_elements = len(data)
        return Constant.TERMINATOR.join([
            datatype + str(num_elements).encode(), Constant.EMPTY_BYTE.join(data)
        ])
    

async def execute_commands(commands: list[str]):
    """
    Execute commands and return the result.
    """

    if not commands:
        return await encode(DataType.SIMPLE_ERROR, Constant.INVALID_COMMAND)
    
    if commands[0].lower() == Command.PING:
        return await encode(DataType.SIMPLE_STRING, Constant.PONG)
    
    if commands[0].lower() == Command.ECHO:        
        return await encode(DataType.SIMPLE_STRING, commands[1].encode())

    if commands[0].lower() == Command.SET:
        key   = commands[1]
        value = commands[2]

        store[key] = {'value': value, 'px': None}

        if len(commands) == 5:
            px = int(commands[4])
            store[key]['px'] = time.time() * 1000 + px

        return await encode(DataType.SIMPLE_STRING, Constant.OK)

    if commands[0].lower() == Command.GET:
        key = commands[1]

        if key in store:
            timestamp_ms = time.time() * 1000
            if not store[key]['px'] or timestamp_ms < store[key]['px']:
                return await encode(DataType.BULK_STRING, store[key]['value'].encode())
                
            store.pop(key)

        return Constant.NULL_BULK_STRING

    if commands[0].lower() == Command.INFO:
        section = commands[1]

        section_config = config.config[section]        
        data = '\n'.join([f'{key}:{value}' for key, value in section_config.items()]).encode()
        
        return await encode(DataType.BULK_STRING, data)

    if commands[0].lower() == Command.REPLCONF:
        return await encode(DataType.SIMPLE_STRING, Constant.OK)
    
    if commands[0].lower() == Command.PSYNC:
        return [
            await encode(
                DataType.SIMPLE_STRING, 
                Constant.SPACE_BYTE.join([
                    Constant.FULLRESYNC, 
                    config.config['replication']['master_replid'].encode(), 
                    str(config.config['replication']['master_repl_offset']).encode()
                ])
            ),
            # note: following is NOT a RESP bulk string, as it doesn't contain a '\r\n' at the end
            Constant.EMPTY_BYTE.join([
                DataType.BULK_STRING, str(len(rdb_state)).encode(), Constant.TERMINATOR, rdb_state
            ]) 
        ]
