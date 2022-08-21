from datetime import date

from dataclasses import dataclass


@dataclass
class InvoiceSummary:
    invoice_id: str
    customer_id: str
    date_created: date
    date_posted: date
    date_due: date
    total_amount: float


@dataclass
class LineItem:
    amount: float
    """The amount of the line item."""
    invoice_id: str
    invoice_line: int
    account_label: str
    """The AR account label to post this item against."""
    location_id: str | None = None
    """Optional location to associate the line item."""
    item_id: str | None = None
    memo: str | None = None
    """A memo pertaining to the line item entry"""


@dataclass
class Invoice:
    invoice_id: str
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

    @property
    def total(self) -> float:
        if self.invoice_items:
            return sum(map(lambda x: x.amount, self.invoice_items))
        else:
            return 0.0

    @property
    def summary(self) -> InvoiceSummary:
        return InvoiceSummary(
            invoice_id=self.invoice_id,
            customer_id=self.customer_id,
            date_created=self.date_created,
            date_posted=self.date_posted,
            date_due=self.date_due,
            total_amount=self.total
        )
