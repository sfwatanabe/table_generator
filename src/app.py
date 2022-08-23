from time import perf_counter

import asyncio
from joblib import Parallel
from src.generators import ErpDataGenerator


async def main(
        batch_size: int = 1_000,
        companies: int = 1_000,
        inv_per_period: int = 1_000,
        n_jobs: int = 8
):

    start = perf_counter()

    await ErpDataGenerator.generate_company_dataset(
        Parallel(n_jobs=n_jobs, verbose=10),
        batch_size,
        companies,
        inv_per_period
    )

    end = perf_counter() - start
    print(f"Total time: {end:.3f}")


if __name__ == '__main__':
    asyncio.run(main())
