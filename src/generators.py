import asyncio
import json
import numpy as np
import pandas as pd
from faker import Faker
from faker.providers import address, internet, person, company, phone_number
from src.models import Contact, MailAddress, Company
from src.utils import serialize_to_camel

fake = Faker('en_US')
fake.add_provider(person)
fake.add_provider(address)
fake.add_provider(company)
fake.add_provider(internet)
fake.add_provider(phone_number)
Faker.seed(42)


async def make_contact(contact_id: int, company_name: str) -> Contact:
    """
    Create a Contact with fake information and mailing address that belongs
    to the supplied company id. Generated names are not guaranteed to be unique
    so we append the current contact id to the appropriate fields. Uniqueness
    constraints are avoided as an optimization for the run time of dataset
    generation.

    Args:
        contact_id (): The contact id for the current Contact.
        company_name (): The company name this Contact will be associated with.

    Returns:
        A Contact with the given id belonging to the specified company.
    """
    full_name = fake.name()
    names = full_name.split(' ')

    return Contact(
        print_as=f"{full_name} {contact_id}",
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


async def make_company(company_id: int) -> Company:
    """
    Create a company with the given id number. Sets the default ar account for
    the company and adds a primary contact.

    Args:
        company_id (): An integer based id value for the current company.

    Returns:
        A Company with a display contact and completed information.
    """
    company_name = fake.company()
    contact = await make_contact(company_id, company_name)

    return Company(
        customer_id=f"C{company_id}",
        name=company_name,
        display_contact=contact,
        status="active",
        ar_account="4000"
    )


async def make_company_batch(start_id: int = 0, batch_size: int = 10) -> list[Company]:
    """
    Create a batch of company objects starting at the given id.

    Processing is done using asyncio.gather to improve performance times.

    Args:
        start_id (): The starting id at which to begin incrementing. The starting
            id will not be in the current list as the ids will be indexed
            by 1. e.x. The first id for `start_id` = 0 will be 1
        batch_size (): The number of companies to generate for the current batch.

    Returns:
        A list of Company objects with fabricated data.
    """
    companies = await asyncio.gather(
        *[make_company(start_id + i + 1) for i in range(batch_size)]
    )
    return companies


async def generate_companies(batch_size: int, total_companies: int) -> list[str]:
    """
    Generates fake company data records and outputs a flattened .csv to the data
    folder. The fake data batch is split into equal sized chunks to generate a
    dataset with the specified total number of companies. If The total number of
    companies to generate is less than the batch size, then only one batch will
    be processed.

    Args:
        batch_size (): The size of each batch to process when creating dataset.
        total_companies (): The total number of company records to create.

    Returns:
        A list of ids for the generated companies.
    """
    batch_ct = total_companies // batch_size
    contact_list = await asyncio.gather(
        *[make_company_batch(i * batch_size, batch_size) for i in range(batch_ct)]
    )

    contact_list = list(np.concatenate(contact_list).flat)
    json_contacts = json.dumps(contact_list, indent=4, default=serialize_to_camel)
    df = pd.json_normalize(json.loads(json_contacts))
    df.to_csv(f"data/company-data.csv", index=False)

    return df['customerId'].to_list()


async def generate_company_dataset(batch_size: int, total_companies: int) -> None:
    """
    Generate a company dataset using the given batch size to create a specified
    number of total companies. The generated datasets will be output to
    `{project-root}/data/` in .csv format. The following dataset files will be
    created during this process

    * company-data
    * invoice-data
    * payment-data

    Args:
        batch_size (): The size of each batch when generating the datasets
        total_companies (): The total number of companies to generate data for.

    Returns:
        None
    """
    # Generate the companies and return a list of ids
    company_list = await generate_companies(batch_size, total_companies)

    # TODO Use the list of ids to create invoices
    print("We're going to make invoices for each company!")

    # TODO Use the list of ids to create payments
    print("We're going to make payments for each company!")

    print(f"Calling some method that creates invoices - {company_list[:20]}")
