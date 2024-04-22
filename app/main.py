import asyncio

async def main():
    server = await asyncio.start_server(lambda _: print('Hello World'), '127.0.0.1', 6379)

if __name__ == "__main__":
    main()
