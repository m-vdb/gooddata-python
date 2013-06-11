

def to_identifier(text):
    if not text:
        return ''
    return text.lower()


def to_title(text):
    if not text:
        return ''
    return text.strip()


def gd_repr(text):
    if isinstance(text, int):
        return str(text)
    if isinstance(text, str):
        return '"%s"' % text.replace('"', '\\"')
    raise NotImplementedError
