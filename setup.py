import os
import distutils.cmd
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


with open(os.path.join(os.path.dirname(__file__), 'README.rst'), encoding='utf-8') as readme:
    README = readme.read().split('h1>\n\n', 2)[1]


setup(
    name='django-postgres-extra',
    version='1.21a16',
    packages=find_packages(),
    include_package_data=True,
    license='MIT License',
    description='Bringing all of PostgreSQL\'s awesomeness to Django.',
    long_description=README,
    url='https://github.com/SectorLabs/django-postgres-extra',
    author='Sector Labs',
    author_email='open-source@sectorlabs.ro',
    keywords=['django', 'postgres', 'extra', 'hstore', 'ltree'],
    classifiers=[
        'Environment :: Web Environment',
        'Framework :: Django',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Topic :: Internet :: WWW/HTTP',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
    ],
    cmdclass={
        'lint': create_command(
            'Lints the code',
            [['flake8', 'setup.py', 'psqlextra', 'tests']],
        ),
    },
)
