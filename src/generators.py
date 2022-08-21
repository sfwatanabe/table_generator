import os
from itertools import cycle
import json
import jsonpickle
import numpy as np
import pandas as pd
from datetime import date, timedelta
from faker import Faker
from faker.providers import address, internet, person, company, date_time, phone_number
from joblib import Parallel, delayed
from src.models import Contact, MailAddress, Company, Invoice, LineItem, Payment, PaymentItem
from src.models import InvoiceSummary
from src.utils import serialize_payment

fake = Faker('en_US')
fake.add_provider(person)
fake.add_provider(address)
fake.add_provider(company)
fake.add_provider(date_time)
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


def write_invoice_period(start_date: date, invoices: list[Invoice]):
    pickled_invoices = jsonpickle.encode(invoices, unpicklable=False)
    df = pd.json_normalize(json.loads(pickled_invoices))
    df.drop(['invoice_items'], axis=1, inplace=True)
    df['amount'] = [i.total for i in invoices]

    df.to_csv(f"data/invoices/invoices_{start_date.year}_{start_date.month}.csv", index=False)


def generate_companies(batch_size: int, total_companies: int) -> list[tuple[str, str]]:
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

    # Concat the results and dump to a file
    df = pd.concat(result_frames)
    df.to_csv(f"data/company-data.csv", index=False)

    # Do some reshaping to return only what we need
    company_info = df[['customer_id', 'display_contact.print_as']]
    company_info.columns = ['company_id', 'contact_name']

    return list(company_info.itertuples(index=False, name='Company'))


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
    date_ranges = []
    for y in range(years_back):
        date_ranges.append(
            (
                date(year=today.year - 2 + y, month=1, day=1),
                date(year=today.year - 2 + y, month=12, day=31)
            )
        )
    date_ranges.append((first_day_this_year, today))

    month_ranges = []
    for start, end in date_ranges:
        month_ranges.append(create_month_ranges(start, end))

    return [month for year in month_ranges for month in year]


def create_payment(invoice_info: InvoiceSummary, payment_id: int,
                   multiple_pct: float = .80) -> Payment:
    # For now we'll just pay in full
    left_to_pay = invoice_info.total_amount
    # Pick a random date within the posted and due date
    date_paid = date_rcv = fake.date_between(invoice_info.date_posted,
                                             invoice_info.date_due)
    payment_items = []

    # Decide if we make multiple payments
    partial_roll = np.random.random_sample()
    if partial_roll > multiple_pct:
        fraction = (.60 - .20) * np.random.random_sample() + 0.20
        item_payment = round(left_to_pay * partial_roll, 2)
        left_to_pay -= item_payment
        payment_items.append(
            PaymentItem(invoice_info.invoice_id, f'{payment_id}', item_payment,
                        date_paid, date_paid)
        )
    # Add a single payment OR the balance to the original
    payment_items.append(
        PaymentItem(invoice_info.invoice_id, f'{payment_id}', round(left_to_pay, 2),
                    date_paid, date_paid)
    )

    payment = Payment(
        payment_id=f'{payment_id}',
        customer_id=invoice_info.customer_id,
        payment_amount=invoice_info.total_amount,
        payment_method='cash',
        base_curr='USD',
        currency_code='USD',
        date_created=date_paid,
        date_received=date_rcv,
        date_posted=date_paid,
        payment_items=payment_items
    )

    return payment


def create_payment_batch(invoices: list[InvoiceSummary], payment_ids: list[int],
                         batch_id: int):
    if len(invoices) != len(payment_ids):
        raise ValueError("Must supply the same number of invoices and payment ids")

    payment_batch = [
        create_payment(i, p_id) for i, p_id in zip(invoices, payment_ids)
    ]
    print("woo let's write these to disk")

    if not os.path.exists('data/payments'):
        os.makedirs('data/payments')

    write_payment_batch(batch_id, payment_batch)
    print(f"Payment batch {batch_id} written to data/payments")


def write_payment_batch(batch_id: int, payments: list[Payment]):
    pickled_payments = [i for p in payments for i in serialize_payment(p)]
    pickled_payments = json.dumps(pickled_payments)

    df = pd.json_normalize(json.loads(pickled_payments))
    df['total_remaining'] = 0

    # TODO Add different serializer for exploding the payment items
    print(f"Payments batch {batch_id} writing to disk")
    df.to_csv(f"data/payments/payments_batch_{batch_id}", index=False)


def generate_payments(invoice_sums: list[list[InvoiceSummary]]):
    # We need some ids! -> 1 for each invoice in the list
    invoice_count = sum(map(len, invoice_sums))
    batches = len(invoice_sums)
    payment_ids = [i + 1 for i in range(invoice_count)]
    payment_ids = np.array_split(payment_ids, batches)

    # Note: We can't pass an iterator to Parallel, it must be an object
    batch_ids = [i + 1 for i in range(batches)]

    Parallel(n_jobs=8, verbose=10)(
        delayed(create_payment_batch)(invoices, ids, batch)
        for batch, invoices, ids
        in zip(batch_ids, invoice_sums, payment_ids)
    )


def create_invoice(company_info: tuple[str, str], invoice_info: tuple[int, float],
                   period: tuple[date, date]) -> Invoice:
    """
    Create an invoice for the given company
    Args:
        company_info (): The company info to assign this invoice to.
        invoice_info (): A tuple of the invoice id and amount to bill.
        period (): The date range to use for the invoice.

    Returns:
        An invoice for the given company id with the provided date and number.
    """
    # Fake a date between the range we have
    fake_date = fake.date_between(period[0], period[1])
    # id, amt = invoice_info

    line_items = [
        LineItem(
            amount=invoice_info[1],
            account_label="4000",
            invoice_id=f"{invoice_info[0]}",
            invoice_line=1
        )
    ]

    return Invoice(
        invoice_id=f"{invoice_info[0]}",
        customer_id=company_info.company_id,
        date_created=fake_date,
        date_posted=fake_date,
        date_due=fake_date + timedelta(days=30),
        base_curr="USD",
        currency_code="USD",
        bill_to_contact_name=f"{company_info.contact_name}",
        ship_to_contact_name=f"{company_info.contact_name}",
        term_name="N30",
        invoice_items=line_items,

    )


def create_invoice_batch(period: tuple[date, date], invoice_ids: list[int],
                         companies: list[tuple[str, str]],
                         low: float = 50.00,
                         high: float = 100000) -> list[Invoice]:
    """
    Create a batch of invoices for the provided company that fall within the
    start and end dates provided.

    Args:
        period (): A tuple of start and end dates.
        invoice_ids (): The invoice ids to use for this batch.
        companies (): The company ids to use for generating this batch.
        low (): The minimum amount for the invoice.
        high (): The maximum amount for the invoice

    Returns:
        A list of invoices for the companies between the given date range.
    """
    # TODO shuffle the invoice ids and split them to the companies
    np.random.shuffle(invoice_ids)
    # TODO Create some invoice amounts between min and max
    amounts = (high - low) * np.random.random_sample(len(invoice_ids)) + low
    amounts = [round(amt, 2) for amt in amounts]
    # Zip up the amounts with the invoices
    invoice_with_amounts = list(zip(invoice_ids, amounts))
    company_cycle = cycle(companies)

    return [
        create_invoice(c, inv, period)
        for c, inv in zip(company_cycle, invoice_with_amounts)
    ]


def generate_invoices(periods: list[list[tuple[date, date]]],
                      companies: list[tuple[str, str]],
                      per_period: int, start_id: int = 0,
                      active_pct: float = .20) -> list[list[InvoiceSummary]]:
    period_count = len(periods)
    invoice_ids = [i + 1 for i in range(start_id, period_count * per_period)]

    # Split the ids into period count
    company_ct = len(companies)
    invoice_ids = np.array_split(invoice_ids, period_count)

    # Sample some indexes
    n_samples = (int(company_ct * active_pct))

    # Grab some random indices
    indices = [
        np.random.choice(company_ct, n_samples)
        for _ in range(period_count)
    ]

    company_samples = [[companies[idx] for idx in idx_arr] for idx_arr in indices]

    # Zip the periods and ids together and start building invoices
    results = Parallel(n_jobs=8, verbose=10)(
        delayed(create_invoice_batch)(period, ids, sample)
        for period, ids, sample in zip(periods, invoice_ids, company_samples)
    )

    Parallel(n_jobs=8, verbose=10)(
        delayed(write_invoice_period)(period[0], i_batch)
        for period, i_batch in zip(periods, results)
    )

    return [[i.summary for i in invoices] for invoices in results]


async def generate_company_dataset(batch_size: int,
                                   total_companies: int,
                                   inv_per_period: int = 500) -> None:
    """
    Generate a company dataset using the given batch size to create a specified
    number of total companies. The generated datasets will be output to
    `{project-root}/data/` in .csv format. The following dataset files will be
    created during this process

    * company-data
    * invoice-data
    * payment-data

    Args:
        batch_size (int): The size of each batch when generating the datasets
        total_companies (int): The total number of companies to generate
        inv_per_period (int): The number of invoices to generate for each period

    Returns:
        None
    """
    # Generate the companies and return a list of ids
    company_list = generate_companies(batch_size, total_companies)

    # For each period we will generate invoices
    period_ranges = create_date_ranges()
    invoice_ids = generate_invoices(period_ranges, company_list, inv_per_period)

    # create_payment_batch(invoice_ids[0],
    #                      [i + 1 for i in range(len(invoice_ids[0]))],
    #                      1)

    print("We're going to make payments for each company!")
    generate_payments(invoice_ids)
