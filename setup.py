import distutils.cmd
import os
import subprocess

from setuptools import find_packages, setup


class BaseCommand(distutils.cmd.Command):
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass


def create_command(text, commands):
    """Creates a custom setup.py command."""

    class CustomCommand(BaseCommand):
        description = text

        def run(self):
            for cmd in commands:
                subprocess.check_call(cmd)

    return CustomCommand


with open(
    os.path.join(os.path.dirname(__file__), "README.rst"), encoding="utf-8"
) as readme:
    README = readme.read().split("h1>\n\n", 2)[1]


setup(
    name="django-postgres-extra",
    version="1.23a1",
    packages=find_packages(),
    include_package_data=True,
    license="MIT License",
    description="Bringing all of PostgreSQL's awesomeness to Django.",
    long_description=README,
    long_description_content_type="text/x-rst",
    url="https://github.com/SectorLabs/django-postgres-extra",
    author="Sector Labs",
    author_email="open-source@sectorlabs.ro",
    keywords=["django", "postgres", "extra", "hstore", "ltree"],
    classifiers=[
        "Environment :: Web Environment",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.5",
        "Topic :: Internet :: WWW/HTTP",
        "Topic :: Internet :: WWW/HTTP :: Dynamic Content",
    ],
    cmdclass={
        "lint": create_command(
            "Lints the code",
            [
                ["flake8", "setup.py", "psqlextra", "tests"],
                ["pycodestyle", "setup.py", "psqlextra", "tests"],
            ],
        ),
        "lint_fix": create_command(
            "Lints the code",
            [
                [
                    "autoflake",
                    "--remove-all-unused-imports",
                    "-i",
                    "-r",
                    "setup.py",
                    "psqlextra",
                    "tests",
                ],
                ["autopep8", "-i", "-r", "setup.py", "psqlextra", "tests"],
            ],
        ),
        "format": create_command(
            "Formats the code", [["black", "setup.py", "psqlextra", "tests"]]
        ),
        "format_verify": create_command(
            "Checks if the code is auto-formatted",
            [["black", "--check", "setup.py", "psqlextra", "tests"]],
        ),
        "sort_imports": create_command(
            "Automatically sorts imports",
            [
                ["isort", "setup.py"],
                ["isort", "-rc", "psqlextra"],
                ["isort", "-rc", "tests"],
            ],
        ),
        "sort_imports_verify": create_command(
            "Verifies all imports are properly sorted.",
            [
                ["isort", "-c", "setup.py"],
                ["isort", "-c", "-rc", "psqlextra"],
                ["isort", "-c", "-rc", "tests"],
            ],
        ),
        "fix": create_command(
            "Automatically format code and fix linting errors",
            [
                ["python", "setup.py", "format"],
                ["python", "setup.py", "sort_imports"],
                ["python", "setup.py", "lint_fix"],
            ],
        ),
        "verify": create_command(
            "Verifies whether the code is auto-formatted and has no linting errors",
            [
                [
                    ["python", "setup.py", "format_verify"],
                    ["python", "setup.py", "sort_imports_verify"],
                    ["python", "setup.py", "lint"],
                ]
            ],
        ),
    },
)
