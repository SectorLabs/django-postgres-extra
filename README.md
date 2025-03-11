# django-pg-extra-extended (Fork for Django 5+)

<h1 align="center">
  <img width="400" src="https://i.imgur.com/79S6OVM.png" alt="django-postgres-extra">
</h1>

This is a fork of `django-postgres-extra`, updated to support Django 5+ while maintaining PostgreSQL enhancements for the Django ORM.

|  |  |  |
|--------------------|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| :memo: | **License** | [![License](https://img.shields.io/:license-mit-blue.svg)](http://doge.mit-license.org) |
| :package: | **PyPi** | (Coming soon) |
| <img src="https://cdn.iconscout.com/icon/free/png-256/django-1-282754.png" width="22px" height="22px" align="center" /> | **Django Versions** | 5.0+ |
| <img src="https://cdn3.iconfinder.com/data/icons/logos-and-brands-adobe/512/267_Python-512.png" width="22px" height="22px" align="center" /> | **Python Versions** | 3.8, 3.9, 3.10, 3.11, 3.12 |
| <img src="https://pbs.twimg.com/profile_images/1152122059/psycopg-100_400x400.png" width="22px" height="22px" align="center" /> | **Psycopg Versions** | 2, 3 |
| :book: | **Documentation** | (Coming soon) |
| :fire: | **Features** | [Features & Documentation](https://github.com/MONSTER-HARSH/django-pg-extra-extended/) |
| :droplet: | **Future enhancements** | [Potential features](https://github.com/MONSTER-HARSH/django-pg-extra-extended/issues?q=is%3Aopen+is%3Aissue+label%3Aenhancement) |

## About

This fork of `django-postgres-extra` extends support for Django 5+, keeping all the powerful PostgreSQL features, including:

- **Native upserts** with bulk support
- **Extended support for HStoreField** (unique constraints, null constraints, etc.)
- **Declarative table partitioning** for PostgreSQL 11+
- **Faster deletes** using table truncation
- **Advanced indexing options** (conditional and case-sensitive unique indexes)

## Installation

Coming soon to PyPI.

For now, install directly from GitHub:

```sh
pip install git+https://github.com/MONSTER-HARSH/django-pg-extra-extended.git
```

## Getting Started

1. Clone the repository:

   ```sh
   git clone https://github.com/MONSTER-HARSH/django-pg-extra-extended.git
   ```

2. Create a virtual environment:

   ```sh
   cd django-pg-extra-extended
   python -m venv env
   source env/bin/activate
   ```

3. Create a PostgreSQL user for testing:

   ```sh
   createuser --superuser psqlextra --pwprompt
   export DATABASE_URL=postgres://psqlextra:<password>@localhost/psqlextra
   ```

4. Install dependencies:

   ```sh
   pip install .[test] .[analysis]
   ```

5. Run tests:

   ```sh
   tox
   ```

## Migration from Original django-postgres-extra

If you're upgrading from `django-postgres-extra` (SectorLabs version), ensure that:

- You update the package source to this fork.
- You check for any API changes due to Django 5+ compatibility adjustments.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

MIT License. See [LICENSE](LICENSE) for details.

