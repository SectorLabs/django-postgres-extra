.. include:: ./snippets/postgres_doc_links.rst
.. include:: ./snippets/manager_model_warning.rst

.. _annotations_page:

Annotations
===========


Renaming annotations
--------------------

Django does not allow you to create an annotation that conflicts with a field on the model. :meth:`psqlextra.query.QuerySet.rename_annotation` makes it possible to do just that.

.. code-block:: python

   from psqlextra.models import PostgresModel
   from django.db.models import Upper

   class MyModel(PostgresModel):
        name = models.TextField()

   MyModel.objects.annotate(name=Upper('name'))

   # OR

   MyModel.objects.annotate(name_upper=Upper('name')).rename_annotations(name='name_upper')
