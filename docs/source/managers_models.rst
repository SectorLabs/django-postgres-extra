.. include:: ./snippets/django_doc_links.rst

.. _managers_models:

Manages & Models
================

:class:`~psqlextra.manager.PostgresManager` exposes a lot of functionality. Your model must use this manager in order to use most of this package's functionality.

There are four ways to do this:

* Inherit your model from :class:`psqlextra.models.PostgresModel`:

   .. code-block:: python

      from psqlextra.models import PostgresModel

      class MyModel(PostgresModel):
          myfield = models.CharField(max_length=255)


* Override default manager with :class:`psqlextra.manager.PostgresManager`:

   .. code-block:: python

      from django.db import models
      from psqlextra.manager import PostgresManager

      class MyModel(models.Model):
          # override default django manager
          objects = PostgresManager()

          myfield = models.CharField(max_length=255)


* Provide :class:`psqlextra.manager.PostgresManager` as a custom manager:

   .. code-block:: python

      from django.db import models
      from psqlextra.manager import PostgresManager

      class MyModel(models.Model):
          # custom mananger name
          beer = PostgresManager()

          myfield = models.CharField(max_length=255)

      # use like this:
      MyModel.beer.upsert(..)

      # not like this:
      MyModel.objects.upsert(..) # error!


* Use the :meth:`psqlextra.util.postgres_manager` on the fly:

    This allows the manager to be used **anywhere** on **any** model, but only within the context. This is especially useful if you want to do upserts into Django's :class:`~django:django.db.models.ManyToManyField` generated :attr:`~django:django.db.models.ManyToManyField.through` table:

   .. code-block:: python

      from django.db import models
      from psqlextra.util import postgres_manager

      class MyModel(models.Model):
          myself = models.ManyToManyField('self')

      # within the context, you can access psqlextra features
      with postgres_manager(MyModel.myself.through) as manager:
          manager.upsert(...)
