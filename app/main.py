import asyncio

async def handler(reader, writer):
    pass

async def main():
    server = await asyncio.start_server(handler, host='127.0.0.1', port=6379)
    await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
