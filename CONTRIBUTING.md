# Contributing

Contributions to `django-postgres-extra` are definitely welcome! Any contribution that implements a PostgreSQL feature in the Django ORM is welcome.

Please use GitHub pull requests to contribute changes.

## 
If you're unsure whether your change would be a good fit for `django-postgres-extra`, please submit an issue with the [idea](https://github.com/SectorLabs/django-postgres-extra/labels/idea) label and we can talk about it.

## Requirements
* All contributions must pass our CI.
  * Existing tests pass.
  * PyLint passes.
  * PEP8 passes.
* Features that allow creating custom indexes or fields must also implement the associated migrations. `django-postgres-extra` prides itself on the fact that it integrates smoothly with Django migrations. We'd like to keep it that way for all features.
* Sufficiently complicated changes must be accomponied by tests.

## Our promise
* We'll promise to reply to each pull request within 24 hours of submission.
* We'll let you know whether we welcome the change or not within that timeframe.
  * This avoids you wasting time on a feature that we feel is not a good fit.

We feel that these promises are fair to whomever decides its worth spending their free time to contribute to `django-postgres-extra`. Please do let us know if you feel we are not living up to these promises.
