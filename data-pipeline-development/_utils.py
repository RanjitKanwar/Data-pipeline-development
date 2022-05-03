
import re
import unicodedata
def sanitize_name(name: str) -> str:
    name = "".join(c for c in unicodedata.normalize("NFD", name) if unicodedata.category(c) != "Mn")  # strip accents
    name = re.sub("[^A-Za-z0-9_]+", "_", name)  # Replacing non alphanumeric characters by underscore
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", name).lower()  # Converting CamelCase to snake_case
