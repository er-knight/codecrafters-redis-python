import asyncio

async def handler(reader, writer):
    _ = await reader.read()

async def main():
    server = await asyncio.start_server(handler, '127.0.0.1', 6379)

if __name__ == "__main__":
    asyncio.run(main())
