import os
import sys
import sphinx_rtd_theme

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "settings")
sys.path.insert(0, os.path.abspath("../.."))

import django
django.setup()

project = "django-postgres-extra"
copyright = "2019-2020, Sector Labs"
author = "Sector Labs"
extensions = [
    "sphinx_rtd_theme",
    "sphinx.ext.intersphinx",
    "sphinx.ext.autodoc",
    "sphinx.ext.coverage",
    "sphinx.ext.napoleon",
]
templates_path = ["_templates"]
exclude_patterns = []
html_theme = "sphinx_rtd_theme"
intersphinx_mapping = {
    "django": ("https://docs.djangoproject.com/en/2.2/", "https://docs.djangoproject.com/en/2.2/_objects/"),
}
