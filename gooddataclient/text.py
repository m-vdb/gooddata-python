

def to_identifier(text):
    if not text:
        return ''
    return text.lower()


def to_title(text):
    if not text:
        return ''
    return text.strip()


def gd_repr(value):
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str):
        return '"%s"' % value.replace('"', '\\"')
    raise NotImplementedError
