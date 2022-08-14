from time import perf_counter

import asyncio
from src.generators import generate_company_dataset


async def main():
    start = perf_counter()
    await generate_company_dataset(1000, 5_000)
    end = perf_counter() - start
    print(f"Total time: {end}")


if __name__ == '__main__':
    asyncio.run(main())
