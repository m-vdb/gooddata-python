

def to_identifier(text):
    if not text:
        return ''
    return text.lower()

def to_title(text):
    if not text:
        return ''
    return text.strip()
