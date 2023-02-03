# Contributing

Contributions to `django-postgres-extra` are definitely welcome! Any contribution that implements a PostgreSQL feature in the Django ORM is welcome.

Please use GitHub pull requests to contribute changes.

##
Information on how to run tests and how to hack on the code can be found at the bottom of the [README](https://github.com/SectorLabs/django-postgres-extra#working-with-the-code).

## 
If you're unsure whether your change would be a good fit for `django-postgres-extra`, please submit an issue with the [idea](https://github.com/SectorLabs/django-postgres-extra/labels/idea) label and we can talk about it.

## Requirements
* All contributions must pass our CI.
  * Existing tests pass.
  * PyLint passes.
  * PEP8 passes.
* Features that allow creating custom indexes or fields must also implement the associated migrations. `django-postgres-extra` prides itself on the fact that it integrates smoothly with Django migrations. We'd like to keep it that way for all features.
* Sufficiently complicated changes must be accompanied by tests.
