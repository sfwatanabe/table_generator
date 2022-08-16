import math

import json
import jsonpickle
import numpy as np
import pandas as pd
from datetime import date, timedelta
from faker import Faker
from faker.providers import address, internet, person, company, phone_number
from joblib import Parallel, delayed
from src.models import Contact, MailAddress, Company, Invoice

fake = Faker('en_US')
fake.add_provider(person)
fake.add_provider(address)
fake.add_provider(company)
fake.add_provider(internet)
fake.add_provider(phone_number)
Faker.seed(42)


def make_contact(contact_id: int, company_name: str) -> Contact:
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


def make_company(company_id: int) -> Company:
    """
    Create a company with the given id number. Sets the default ar account for
    the company and adds a primary contact.

    Args:
        company_id (): An integer based id value for the current company.

    Returns:
        A Company with a display contact and completed information.
    """
    company_name = fake.company()
    contact = make_contact(company_id, company_name)

    return Company(
        customer_id=f"C{company_id}",
        name=company_name,
        display_contact=contact,
        status="active",
        ar_account="4000"
    )


def make_company_batch(start_id: int = 0, batch_size: int = 10) -> pd.DataFrame:
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

    companies = [make_company(start_id + i + 1) for i in range(batch_size)]
    pickled_stuff = jsonpickle.encode(companies, unpicklable=False)
    return pd.json_normalize(json.loads(pickled_stuff))


def generate_companies(batch_size: int, total_companies: int) -> list[str]:
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
    item_list = [i * batch_size for i in range(batch_ct)]
    result_frames = Parallel(n_jobs=8, verbose=10)(
        delayed(make_company_batch)(i, batch_size)
        for i in item_list
    )

    df = pd.concat(result_frames)

    df.to_csv(f"data/company-data.csv", index=False)
    return df['customer_id'].to_list()


def get_period_end(start_date: date) -> date:
    if start_date.month == 12:
        return date(year=start_date.year, month=start_date.month, day=31)
    else:
        return date(year=start_date.year, month=start_date.month + 1, day=1) \
               + timedelta(days=-1)


def create_month_ranges(start_date: date, end_date: date) -> list[tuple[date, date]]:
    """
    Create a list of date ranges between the start and end date.

    Args:
        start_date (): The start date to use for the date ranges.
        end_date (): The last date in the date range

    Returns:
        A list of tuples with start and end dates representing the months that
        are between the start and end date.
    """
    if start_date > end_date:
        return []

    period_begin = start_date
    period_end = get_period_end(start_date)
    month_ranges = []

    for m in range(end_date.month):
        if m + 1 == end_date.month:
            month_ranges.append((
                period_begin, end_date
            ))
        else:
            month_ranges.append((
                period_begin, period_end
            ))

            # Update the period begin and end dates
            period_begin = date(period_begin.year, period_begin.month + 1, 1)
            period_end = get_period_end(period_begin)
    return month_ranges


def create_date_ranges(years_back: int = 2) -> list[list[tuple[date, date]]]:
    """
    Generate a list of date range objects that have a start and end date for
    the previous number of years and the current year up to the current date.

    Args:
        years_back (): The number of years back to create date ranges for.

    Returns:
        A list of date ranges containing tuples where the elements represent the
        start and end dates for a given period.
    """
    today = date.today()
    first_day_this_year = date(year=today.year, month=1, day=1)
    date_ranges = [(first_day_this_year, today)]
    for y in range(years_back):
        date_ranges.append(
            (
                date(year=today.year - y - 1, month=1, day=1),
                date(year=today.year - y - 1, month=12, day=31)
            )
        )

    month_ranges = []
    for start, end in date_ranges:
        month_ranges.append(create_month_ranges(start, end))

    return month_ranges


async def create_invoice(invoice_no: int, company_id: str, invoice_date: date) -> Invoice:
    """
    Create an invoice for the given company
    Args:
        invoice_no (): The invoice number to use for this invoice.
        company_id (): The company id to assign this invoice to.
        invoice_date (): The date to use for the invoice.

    Returns:
        An invoice for the given company id with the provided date and number.
    """
    pass


async def create_invoice_batch(company_id: str, starting_no: int, count: int,
                               start_date: date, end_date: date) -> list[Invoice]:
    """
    Create a batch of invoices for the provided company that fall within the
    start and end dates provided.

    Args:
        company_id (): The company id to assign these invoices to .
        starting_no (): The starting invoice number for this batch.
        count (): The total number of invoices to create for the company.
        start_date (): The beginning date for the invoice range.
        end_date (): The ending date for the invoice range.

    Returns:
        A list of invoices for the company that are between the given date range.
    """
    pass


async def generate_invoices(period_ranges: list[list[tuple[date, date]]], start_id: int = 0,
                            period_size: int = 20, total_invoices=10000):
    invoice_ids = [i + 1 for i in range(start_id, total_invoices)]

    period_count = sum(map(len, period_ranges))
    chunk_count = math.ceil(total_invoices / period_count)
    invoice_ids = [
        list(chunk) for chunk in np.array_split(invoice_ids, chunk_count)
    ]
    print(invoice_ids)


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
    company_list = generate_companies(batch_size, total_companies)

    # We're making dates!
    period_ranges = create_date_ranges()
    # For each period we will generate invoices
    # we can just call to generate invoice with the company list and total per period to issue
    await generate_invoices(period_ranges)

    # TODO Use the list of ids to create invoices
    print("We're going to make invoices for each company!")

    # TODO Use the list of ids to create payments
    print("We're going to make payments for each company!")

    # print(f"Calling some method that creates invoices - {company_list[:20]}")
