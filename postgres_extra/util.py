from typing import List

from django.conf import settings


def get_language_codes() -> List[str]:
    """Gets a list of all available language codes.

    This looks at your project's settings.LANGUAGES
    and returns a flat list of the configured
    language codes.

    Arguments:
        A flat list of all availble language codes
        in your project.
    """

    return [
        lang_code
        for lang_code, _ in settings.LANGUAGES
    ]
