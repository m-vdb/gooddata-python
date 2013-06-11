

def to_identifier(text):
    if not text:
        return ''
    return text.lower()


def to_title(text):
    if not text:
        return ''
    return text.strip()


def gd_repr(text):
    #wrong way
    #return repr(text).replace("'", '"')
    if isinstance(text, int):
        return str(text)
    if isinstance(text, str):
        return '"%s"' % text
    raise NotImplementedError
