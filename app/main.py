import asyncio
import argparse

from . import resp
from . import config


async def send_handshake(address):
    host, port = address
    reader, writer = await asyncio.open_connection(host=host, port=port)

    writer.write(
        await resp.encode(resp.DataType.ARRAY, [
            await resp.encode(resp.DataType.BULK_STRING, resp.Command.PING.encode())
        ])        
    )
    await writer.drain()

    await resp.parse_response()

    writer.write(
        await resp.encode(resp.DataType.ARRAY, [
            await resp.encode(resp.DataType.BULK_STRING, resp.Command.REPLCONF.encode()),
            await resp.encode(resp.DataType.BULK_STRING, 'listening-port'.encode()),
            await resp.encode(resp.DataType.BULK_STRING, '6380'.encode())
        ])        
    )
    await writer.drain()

    await resp.parse_response()

    writer.write(
        await resp.encode(resp.DataType.ARRAY, [
            await resp.encode(resp.DataType.BULK_STRING, resp.Command.REPLCONF.encode()),
            await resp.encode(resp.DataType.BULK_STRING, 'capa'.encode()),
            await resp.encode(resp.DataType.BULK_STRING, 'psync2'.encode())
        ])        
    )
    await writer.drain()

    await resp.parse_response()

    # writer.write(
    #     await resp.encode(resp.DataType.ARRAY, [
    #         await resp.encode(resp.DataType.BULK_STRING, resp.Command.PSYNC.encode()),
    #         await resp.encode(resp.DataType.BULK_STRING, '?'.encode()),
    #         await resp.encode(resp.DataType.BULK_STRING, '-1'.encode())
    #     ])        
    # )
    # await writer.drain()


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        commands = await resp.parse_commands(reader)
        result   = await resp.execute_commands(commands) 
        writer.write(result)
        await writer.drain()


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)
    parser.add_argument('--replicaof', nargs=2, type=str)

    args = parser.parse_args()

    HOST = '127.0.0.1'
    PORT = args.port or 6379

    if args.replicaof:
        config.config['replication']['role'] = 'slave'
        await send_handshake(args.replicaof)

    server = await asyncio.start_server(handler, host=HOST, port=PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
