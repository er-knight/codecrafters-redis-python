import asyncio

async def handler(reader, writer):
    data = await reader.read()
    print(data)
    writer.write('+PONG\r\n'.encode())
    await writer.drain() 

async def main():
    server = await asyncio.start_server(handler, host='127.0.0.1', port=6379)
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
