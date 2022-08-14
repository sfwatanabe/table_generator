from datetime import date

from dataclasses import dataclass


@dataclass
class LineItem:
    amount: float
    """The amount of the line item."""
    account_label: str
    """The AR account label to post this item against."""
    location_id: str | None = None
    """Optional location to associate the line item."""
    item_id: str | None = None
    memo: str | None = None
    """A memo pertaining to the line item entry"""


@dataclass
class Invoice:
    invoice_no: str
    customer_id: str
    """Customer id must match with a customer from the generated dataset"""
    date_created: date
    date_posted: date
    date_due: date
    base_curr: str
    currency_code: str
    bill_to_contact_name: str
    ship_to_contact_name: str
    term_name: str | None
    po_number: str | None = None
    invoice_items: list[LineItem] | None = None