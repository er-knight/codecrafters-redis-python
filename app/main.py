import asyncio

async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    command = await reader.readline()
    print(command)
    writer.write('+PONG\r\n'.encode())
    await writer.drain() 

async def main():
    server = await asyncio.start_server(handler, host='127.0.0.1', port=6379)
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
