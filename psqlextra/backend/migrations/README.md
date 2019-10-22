## What's up with the shady patch functions?
Django currently does not provide a way to extend certain classes that are used when auto-generating migrations using `makemigrations`. The patch functions use Python's standard mocking framework to direct certain functions to a custom implementation.

These patches allow `django-postgres-extra` to let Django auto-generate migrations for `PostgresPartitionedModel`, `PostgresViewModel` and `PostgresMaterializedView`.

None of the patches fundamentally change how Django work. They let Django do most of the work and only customize for Postgres specific models. All of the patches call the original implementation and then patch the results instead of copying the entire implementation.

### Using the patches
The patches are all context managers. The top level `postgres_patched_migrations` context manager applies all patches for the duration of the context.

This is used in the custom `pgmakemigrations` command to extend the migration autodetector for `PostgresPartitionedModel`, `PostgresViewModel` and `PostgresMaterializedView`.

### Patches
#### Autodetector patch
* Patches `django.db.migrations.autodetector.MigrationAutodetector.add_operation`

This function is called every time the autodetector adds a new operation. For example, if Django detects a new model, `add_operation` is called with a new `CreateModel` operation instance.

The patch hooks into the `add_operation` function to transform the following operations:

* `Createmodel` into a `PostgresCreatePartitionedModel` operation if the model is a `PostgresPartitionedModel` and adds a `PostgresAddDefaultPartition` operation to create a default partition.

* `DeleteModel` into a `PostgresDeletePartitionedModel` operation if the model is a `PostgresPartitionedModel`.

* `CreateModel` into a `PostgresCreateViewModel` operation if the model is a `PostgresViewModel`.

* `DeleteModel` into a `PostgresDeleteviewModel` operation if the model is a `PostgresViewModel`.

* `CreateModel` into a `PostgresCreateMaterializedViewModel` operation if the model is a `PostgresMaterializedViewModel`.

* `DeleteModel` into a `PostgresDeleteMaterializedViewModel` operation if the model is a `PostgresMaterializedViewModel`.

* `AddField` into `ApplyState` migration if the model is a `PostgresViewModel` or `PostgresMaterializedViewModel`.

* `AlterField` into `ApplyState` migration if the model is a `PostgresViewModel` or `PostgresMaterializedViewModel`.

* `RenameField` into `ApplyState` migration if the model is a `PostgresViewModel` or `PostgresMaterializedViewModel`.

* `RemoveField` into `ApplyState` migration if the model is a `PostgresViewModel` or `PostgresMaterializedViewModel`.

#### ProjectState patch
* Patches `django.db.migrations.state.ProjectState.from_apps`

This function is called to build up the current migration state from all the installed apps. For each model, a `ModelState` is created.

The patch hooks into the `from_apps` function to transform the following:

* Create `PostgresPartitionedModelState` from the model if the model is a `PostgresPartitionedModel`.
* Create `PostgresViewModelState` from the model if the model is a `PostgresViewModel`.
* Create `PostgresMaterializedViewModelState` from the model if the model is a `PostgresMaterializedViewModel`.

These custom model states are needed to track partitioning and view options (`PartitioningMeta` and `ViewMeta`) in migrations. Without this, the partitioning and view optiosn would not end up in migrations.
