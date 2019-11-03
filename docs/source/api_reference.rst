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

.. automodule:: psqlextra.indexes

   .. autoclass:: ConditionalUniqueIndex
   .. autoclass:: CaseInsensitiveUniqueIndex

.. automodule:: psqlextra.types
   :members:
   :undoc-members:

.. automodule:: psqlextra.util
   :members:
