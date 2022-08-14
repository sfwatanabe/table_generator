from dataclasses import dataclass
from src.models import Contact


@dataclass
class Company:
    customer_id: str
    name: str
    display_contact: Contact
    status: str
    ar_account: str

