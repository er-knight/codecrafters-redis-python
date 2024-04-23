import asyncio
import argparse

from . import resp


async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        commands = await resp.parse(reader)
        result   = await resp.execute(commands) 
        writer.write(result)
        await writer.drain()


async def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int)

    args = parser.parse_args()

    HOST = '127.0.0.1'
    PORT = args.port or 6379

    server = await asyncio.start_server(handler, host=HOST, port=PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
