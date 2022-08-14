from dataclasses import dataclass
from src.models._mail_address import MailAddress


@dataclass
class Contact:
    print_as: str
    first_name: str
    last_name: str
    company_name: str
    email1: str | None = None
    phone1: str | None = None
    phone2: str | None = None
    mail_address: MailAddress | None = None
