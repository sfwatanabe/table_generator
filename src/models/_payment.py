from datetime import date
from dataclasses import dataclass


@dataclass
class PaymentItem:
    """
    An application of a payment to an invoice.
    """
    invoice_id: str
    payment_id: str
    amount: float
    date_created: date
    date_posted: date


@dataclass
class Payment:
    """
    A single payment item made by a customer to the company
    """
    customer_id: str
    payment_id: str
    payment_amount: float
    payment_method: str
    base_curr: str
    currency_code: str
    date_created: date
    date_received: date
    date_posted: date
    reference_id: str | None = None
    payment_items: list[PaymentItem] | None = None

    @property
    def total_remaining(self) -> float:
        """
        The amount of this payment still un-applied.

        Returns:
            The total amount still available to apply to invoices.
        """
        if not self.payment_items:
            return 0
        item_totals = sum(map(lambda x: x.amount, self.payment_items))
        return self.payment_amount - item_totals

    @property
    def summary(self) -> dict:
        return {
            "payment_id": self.payment_id,
            "customer_id": self.customer_id,
            "payment_amount": self.payment_amount,
            "total_remaining": self.total_remaining,
            "date_created": str(self.date_created),
            "date_received": str(self.date_received),
            "date_posted": str(self.date_posted)
        }
