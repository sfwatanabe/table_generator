from time import perf_counter

import asyncio
from src.generators import generate_company_dataset


async def main():
    start = perf_counter()
    # TODO Set optional parameters for batch size, total companies, n-jobs
    await generate_company_dataset(1000, 10_000)
    end = perf_counter() - start
    print(f"Total time: {end}")


if __name__ == '__main__':
    asyncio.run(main())
