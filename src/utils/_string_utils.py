from src.models import Payment


def serialize_to_camel(obj):
    """
    Convert the given object into an object with camel cased property labels
    """
    return {snake_to_camel(k): v for k, v in obj.__dict__.items()}


def serialize_payment(payment: Payment) -> list[dict]:
    if not payment.payment_items:
        return {}
    base_payment = payment.summary

    results = []
    for p_item in payment.payment_items:
        item_details = {"invoice_id": p_item.invoice_id, "line_amount": p_item.amount}
        item_details.update(base_payment)
        results.append(item_details)

    return results


def snake_to_camel(string: str) -> str:
    """
    Converts the snake case to a camel cased string.

    >>> snake_to_camel("snake_case_item")
    'snakeCaseItem'

    >>> snake_to_camel("this_property")
    'thisProperty'

    """
    words = string.split('_')
    words[0] = words[0].lower()

    if len(words) > 1:
        words[1:] = [word.title() for word in words[1:]]
    return ''.join(words)
