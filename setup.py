import os

from setuptools import find_packages, setup

exec(open("psqlextra/_version.py").read())

with open(
    os.path.join(os.path.dirname(__file__), "README.md"), encoding="utf-8"
) as readme:
    README = readme.read().split("h1>\n", 2)[1]


setup(
    name="django-postgres-extra",
    version=__version__,  # noqa
    packages=find_packages(exclude=["tests"]),
    package_data={"psqlextra": ["py.typed"]},
    include_package_data=True,
    license="MIT License",
    description="Bringing all of PostgreSQL's awesomeness to Django.",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/SectorLabs/django-postgres-extra",
    author="Sector Labs",
    author_email="open-source@sectorlabs.ro",
    keywords=[
        "django",
        "postgres",
        "extra",
        "hstore",
        "upsert",
        "partioning",
        "materialized",
        "view",
    ],
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    python_requires=">=3.6",
    install_requires=[
        "Django>=2.0,<6.0",
        "python-dateutil>=2.8.0,<=3.0.0",
    ],
    extras_require={
        # Python 3.6 - Python 3.13
        ':python_version <= "3.6"': ["dataclasses"],
        "dev": [
            "poethepoet==0.34.0; python_version >= '3.9'",
            "poethepoet==0.30.0; python_version >= '3.8' and python_version < '3.9'",
            "poethepoet==0.19.0; python_version >= '3.7' and python_version < '3.8'",
            "poethepoet==0.13.1; python_version >= '3.6' and python_version < '3.7'",
        ],
        "test": [
            "psycopg2==2.9.10; python_version >= '3.8'",
            "psycopg2==2.9.9; python_version >= '3.7' and python_version < '3.8'",
            "psycopg2==2.9.8; python_version >= '3.6' and python_version < '3.7'",
            "types-psycopg2==2.9.21.20250516; python_version >= '3.9'",
            "types-psycopg2==2.9.8; python_version >= '3.6' and python_version < '3.9'",
            "pytest==8.4.0; python_version > '3.8'",
            "pytest==7.0.1; python_version <= '3.8'",
            "pytest-benchmark==5.1.0; python_version > '3.8'",
            "pytest-benchmark==3.4.1; python_version <= '3.8'",
            "pytest-django==4.11.1; python_version > '3.7'",
            "pytest-django==4.5.2; python_version <= '3.7'",
            "pytest-cov==6.1.1; python_version > '3.8'",
            "pytest-cov==4.0.0; python_version <= '3.8'",
            "coverage==7.8.2; python_version > '3.8'",
            "coverage==7.6.1; python_version >= '3.8' and python_version <= '3.8'",
            "coverage==6.2; python_version <= '3.7'",
            "tox==4.26.0; python_version > '3.8'",
            "tox==3.28.0; python_version <= '3.8'",
            "freezegun==1.5.2; python_version > '3.7'",
            "freezegun==1.2.2; python_version <= '3.7'",
            "syrupy==4.9.1; python_version >= '3.9'",
            "syrupy==2.3.1; python_version <= '3.8'",
        ],
        # Python 3.11 assumed from below
        "test-report": ["coveralls==4.0.1"],
        "analysis": [
            "black==22.3.0",
            "flake8==7.2.0",
            "autoflake==2.3.1",
            "autopep8==2.3.2",
            "isort==6.0.1",
            "docformatter==1.7.7",
            "mypy==1.16.0",
            "django-stubs==4.2.7",
            "typing-extensions==4.14.0",
            "types-dj-database-url==1.3.0.4",
            "types-python-dateutil==2.9.0.20250516",
        ],
        "docs": [
            "Sphinx==8.2.3",
            "sphinx-rtd-theme==3.0.2",
            "docutils==0.21.2",
            "Jinja2==3.1.6",
        ],
        "publish": [
            "build==0.7.0",
            "twine==3.7.1",
        ],
    },
)
