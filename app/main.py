import asyncio

from . import resp

async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    while True:
        commands = await resp.parse(reader)
        result   = await resp.execute(commands) 
        print(result)
        writer.write(result)
        await writer.drain()

async def main():
    server = await asyncio.start_server(handler, host='127.0.0.1', port=6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
