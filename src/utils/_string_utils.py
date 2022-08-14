
def serialize_to_camel(obj):
    """
    Convert the given object into an object with camel cased property labels
    """
    return {snake_to_camel(k): v for k, v in obj.__dict__.items()}


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

