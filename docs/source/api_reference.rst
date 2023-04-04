API Reference
-------------

.. automodule:: psqlextra.manager

   .. autoclass:: PostgresManager
      :members:

.. automodule:: psqlextra.query

   .. autoclass:: PostgresQuerySet
      :members:
      :exclude-members: annotate, rename_annotations

.. automodule:: psqlextra.models
    :members:

.. automodule:: psqlextra.fields

   .. autoclass:: HStoreField
      :members:
      :exclude-members: deconstruct, get_prep_value

      .. automethod:: __init__

.. automodule:: psqlextra.expressions

   .. autoclass:: HStoreRef

   .. autoclass:: DateTimeEpoch

   .. autoclass:: ExcludedCol

.. automodule:: psqlextra.indexes

   .. autoclass:: UniqueIndex

   .. autoclass:: ConditionalUniqueIndex

   .. autoclass:: CaseInsensitiveUniqueIndex

.. automodule:: psqlextra.locking
   :members:

.. automodule:: psqlextra.schema
   :members:

.. automodule:: psqlextra.partitioning
   :members:

.. automodule:: psqlextra.backend.migrations.operations
   :members:

.. automodule:: psqlextra.types
   :members:
   :undoc-members:

.. automodule:: psqlextra.util
   :members:
