import os
from time import perf_counter

import asyncio
import json
import numpy as np
import pandas as pd
from faker import Faker
from faker.providers import address, internet, person, company, phone_number
from src.models import Contact, Company
from src.models import MailAddress
from src.utils import serialize_to_camel

fake = Faker('en_US')
fake.add_provider(person)
fake.add_provider(address)
fake.add_provider(company)
fake.add_provider(internet)
fake.add_provider(phone_number)
Faker.seed(42)


async def make_contact(contact_id: int, company_name: str) -> Contact:
    """Make a generic contact with the given id number"""
    # company_name = fake.company()
    full_name = fake.name()
    names = full_name.split(' ')

    return Contact(
        print_as=f"Contact Number {contact_id}",
        # first_name=f"First{contact_id}",
        # last_name=f"Last{contact_id}",
        first_name=f"{names[0]}",
        last_name=f"{names[1]} {contact_id}",
        company_name=f"{company_name}-{contact_id}",
        email1=fake.safe_email(),
        phone1=fake.phone_number(),
        phone2=fake.phone_number(),
        mail_address=MailAddress(
            address1=fake.street_address(),
            address2=f"Attn: {contact_id}",
            city=fake.city(),
            state=fake.state(),
            country=fake.current_country_code(),
            zip=fake.postcode()
        )
    )


async def make_company(contact_id: int) -> Company:
    """Make a generic Company with the given id number"""
    company_name = fake.company()
    contact = await make_contact(contact_id, company_name)

    return Company(
        customer_id=f"C{contact_id}",
        name=company_name,
        display_contact=contact,
        status="active",
        ar_account="4000"
    )


async def make_company_batch(start_id: int = 0, list_size: int = 10) -> list[Company]:
    """
    Make a list of n contacts starting with the given id.
    """
    the_contacts = await asyncio.gather(
        *[make_company(start_id + i + 1) for i in range(list_size)]
    )
    return the_contacts


async def generate_companies(batch_size: int, total_contacts: int):
    """
    Generate a dataframe filled with flattened contacts and export to csv.

    returns: An array of created company ids.
    """
    batch_ct = total_contacts // batch_size
    contact_list = await asyncio.gather(
        *[make_company_batch(i * batch_size, batch_size) for i in range(batch_ct)]
    )

    contact_list = list(np.concatenate(contact_list).flat)
    json_contacts = json.dumps(contact_list, indent=4, default=serialize_to_camel)
    df = pd.json_normalize(json.loads(json_contacts))
    df.to_csv(f"data/company-data.csv", index=False)
    return df['customerId']


async def main():
    start = perf_counter()
    df = await generate_companies(1000, 50_000)
    end = perf_counter() - start
    print(f"Total time: {end}")


if __name__ == '__main__':
    asyncio.run(main())
