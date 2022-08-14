from time import perf_counter

import asyncio
from src.generators import generate_companies


async def main():
    start = perf_counter()
    # TODO we can move these constants to a settings or .env file later
    df = await generate_companies(1000, 5_000)
    end = perf_counter() - start
    print(f"Total time: {end}")


if __name__ == '__main__':
    asyncio.run(main())
