from dataclasses import dataclass
from src.models import Contact


@dataclass
class Company:
    customer_id: str
    company_name: str
    display_contact: Contact
