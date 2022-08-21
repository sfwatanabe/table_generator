from time import perf_counter

import asyncio
from src.generators import generate_company_dataset

COMPANY_BATCH_SIZE = 1000


async def main(num_companies: int = 1_000):
    start = perf_counter()
    # TODO Set optional parameters for batch size, total companies, n-jobs
    await generate_company_dataset(COMPANY_BATCH_SIZE, num_companies)
    end = perf_counter() - start
    print(f"Total time: {end:.3f}")


if __name__ == '__main__':
    asyncio.run(main())
