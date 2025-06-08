import django
import freezegun
import pytest

from django.contrib.postgres.fields import ArrayField
from django.db import connection, models
from django.test.utils import CaptureQueriesContext
from django.utils import timezone

from psqlextra.introspect import model_from_cursor, models_from_cursor

from .fake_model import get_fake_model

django_31_skip_reason = "Django < 3.1 does not support JSON fields which are required for these tests"


@pytest.fixture
def mocked_model_varying_fields():
    return get_fake_model(
        {
            "title": models.TextField(null=True),
            "updated_at": models.DateTimeField(null=True),
            "content": models.JSONField(null=True),
            "items": ArrayField(models.TextField(), null=True),
        }
    )


@pytest.fixture
def mocked_model_single_field():
    return get_fake_model(
        {
            "name": models.TextField(),
        }
    )


@pytest.fixture
def mocked_model_foreign_keys(
    mocked_model_varying_fields, mocked_model_single_field
):
    return get_fake_model(
        {
            "varying_fields": models.ForeignKey(
                mocked_model_varying_fields, null=True, on_delete=models.CASCADE
            ),
            "single_field": models.ForeignKey(
                mocked_model_single_field, null=True, on_delete=models.CASCADE
            ),
        }
    )


@pytest.fixture
def mocked_model_varying_fields_instance(mocked_model_varying_fields):
    with freezegun.freeze_time("2020-1-1 12:00:00.0"):
        return mocked_model_varying_fields.objects.create(
            title="hello world",
            updated_at=timezone.now(),
            content={"a": 1},
            items=["a", "b"],
        )


@pytest.fixture
def models_from_cursor_wrapper_multiple():
    def _wrapper(*args, **kwargs):
        return list(models_from_cursor(*args, **kwargs))[0]

    return _wrapper


@pytest.fixture
def models_from_cursor_wrapper_single():
    return model_from_cursor


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_applies_converters(
    request,
    mocked_model_varying_fields,
    mocked_model_varying_fields_instance,
    models_from_cursor_wrapper_name,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    with connection.cursor() as cursor:
        cursor.execute(
            *mocked_model_varying_fields.objects.all().query.sql_with_params()
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_varying_fields, cursor
        )

    assert queried_instance.id == mocked_model_varying_fields_instance.id
    assert queried_instance.title == mocked_model_varying_fields_instance.title
    assert (
        queried_instance.updated_at
        == mocked_model_varying_fields_instance.updated_at
    )
    assert (
        queried_instance.content == mocked_model_varying_fields_instance.content
    )
    assert queried_instance.items == mocked_model_varying_fields_instance.items


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_handles_field_order(
    request,
    mocked_model_varying_fields,
    mocked_model_varying_fields_instance,
    models_from_cursor_wrapper_name,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    with connection.cursor() as cursor:
        cursor.execute(
            f'SELECT content, items, id, title, updated_at FROM "{mocked_model_varying_fields._meta.db_table}"',
            tuple(),
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_varying_fields, cursor
        )

    assert queried_instance.id == mocked_model_varying_fields_instance.id
    assert queried_instance.title == mocked_model_varying_fields_instance.title
    assert (
        queried_instance.updated_at
        == mocked_model_varying_fields_instance.updated_at
    )
    assert (
        queried_instance.content == mocked_model_varying_fields_instance.content
    )
    assert queried_instance.items == mocked_model_varying_fields_instance.items


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_handles_partial_fields(
    request,
    mocked_model_varying_fields,
    mocked_model_varying_fields_instance,
    models_from_cursor_wrapper_name,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    with connection.cursor() as cursor:
        cursor.execute(
            f'SELECT id FROM "{mocked_model_varying_fields._meta.db_table}"',
            tuple(),
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_varying_fields, cursor
        )

    assert queried_instance.id == mocked_model_varying_fields_instance.id
    assert queried_instance.title is None
    assert queried_instance.updated_at is None
    assert queried_instance.content is None
    assert queried_instance.items is None


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_handles_null(
    request, mocked_model_varying_fields, models_from_cursor_wrapper_name
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    instance = mocked_model_varying_fields.objects.create()

    with connection.cursor() as cursor:
        cursor.execute(
            *mocked_model_varying_fields.objects.all().query.sql_with_params()
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_varying_fields, cursor
        )

    assert queried_instance.id == instance.id
    assert queried_instance.title is None
    assert queried_instance.updated_at is None
    assert queried_instance.content is None
    assert queried_instance.items is None


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_foreign_key(
    request,
    mocked_model_single_field,
    mocked_model_foreign_keys,
    models_from_cursor_wrapper_name,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    instance = mocked_model_foreign_keys.objects.create(
        varying_fields=None,
        single_field=mocked_model_single_field.objects.create(name="test"),
    )

    with connection.cursor() as cursor:
        cursor.execute(
            *mocked_model_foreign_keys.objects.all().query.sql_with_params()
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_foreign_keys, cursor
        )

    with CaptureQueriesContext(connection) as ctx:
        assert queried_instance.id == instance.id
        assert queried_instance.varying_fields_id is None
        assert queried_instance.varying_fields is None
        assert queried_instance.single_field_id == instance.single_field_id
        assert queried_instance.single_field.id == instance.single_field.id
        assert queried_instance.single_field.name == instance.single_field.name

        assert len(ctx.captured_queries) == 1


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
def test_models_from_cursor_related_fields(
    request,
    mocked_model_varying_fields,
    mocked_model_single_field,
    mocked_model_foreign_keys,
    models_from_cursor_wrapper_name,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    instance = mocked_model_foreign_keys.objects.create(
        varying_fields=mocked_model_varying_fields.objects.create(
            title="test", updated_at=timezone.now()
        ),
        single_field=mocked_model_single_field.objects.create(name="test"),
    )

    with connection.cursor() as cursor:
        cursor.execute(
            *mocked_model_foreign_keys.objects.select_related(
                "varying_fields", "single_field"
            )
            .all()
            .query.sql_with_params()
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_foreign_keys,
            cursor,
            related_fields=["varying_fields", "single_field"],
        )

    with CaptureQueriesContext(connection) as ctx:
        assert queried_instance.id == instance.id

        assert queried_instance.varying_fields_id == instance.varying_fields_id
        assert queried_instance.varying_fields.id == instance.varying_fields.id
        assert (
            queried_instance.varying_fields.title
            == instance.varying_fields.title
        )
        assert (
            queried_instance.varying_fields.updated_at
            == instance.varying_fields.updated_at
        )
        assert (
            queried_instance.varying_fields.content
            == instance.varying_fields.content
        )
        assert (
            queried_instance.varying_fields.items
            == instance.varying_fields.items
        )

        assert queried_instance.single_field_id == instance.single_field_id
        assert queried_instance.single_field.id == instance.single_field.id
        assert queried_instance.single_field.name == instance.single_field.name

        assert len(ctx.captured_queries) == 0


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
@pytest.mark.parametrize(
    "models_from_cursor_wrapper_name",
    [
        "models_from_cursor_wrapper_multiple",
        "models_from_cursor_wrapper_single",
    ],
)
@pytest.mark.parametrize(
    "selected", [True, False], ids=["selected", "not_selected"]
)
def test_models_from_cursor_related_fields_optional(
    request,
    mocked_model_varying_fields,
    mocked_model_foreign_keys,
    models_from_cursor_wrapper_name,
    selected,
):
    models_from_cursor_wrapper = request.getfixturevalue(
        models_from_cursor_wrapper_name
    )

    instance = mocked_model_foreign_keys.objects.create(
        varying_fields=mocked_model_varying_fields.objects.create(
            title="test", updated_at=timezone.now()
        ),
        single_field=None,
    )

    with connection.cursor() as cursor:
        select_related = ["varying_fields"]
        if selected:
            select_related.append("single_field")

        cursor.execute(
            *mocked_model_foreign_keys.objects.select_related(*select_related)
            .all()
            .query.sql_with_params()
        )
        queried_instance = models_from_cursor_wrapper(
            mocked_model_foreign_keys,
            cursor,
            related_fields=["varying_fields", "single_field"],
        )

    assert queried_instance.id == instance.id
    assert queried_instance.varying_fields_id == instance.varying_fields_id
    assert queried_instance.single_field_id == instance.single_field_id

    with CaptureQueriesContext(connection) as ctx:
        assert queried_instance.varying_fields.id == instance.varying_fields.id
        assert (
            queried_instance.varying_fields.title
            == instance.varying_fields.title
        )
        assert (
            queried_instance.varying_fields.updated_at
            == instance.varying_fields.updated_at
        )
        assert (
            queried_instance.varying_fields.content
            == instance.varying_fields.content
        )
        assert (
            queried_instance.varying_fields.items
            == instance.varying_fields.items
        )

        assert queried_instance.single_field is None

        assert len(ctx.captured_queries) == 0


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
def test_models_from_cursor_generator_efficiency(
    mocked_model_varying_fields, mocked_model_single_field
):
    mocked_model_single_field.objects.create(name="a")
    mocked_model_single_field.objects.create(name="b")

    with connection.cursor() as cursor:
        cursor.execute(
            *mocked_model_single_field.objects.all().query.sql_with_params()
        )

        instances_generator = models_from_cursor(
            mocked_model_single_field, cursor
        )
        assert cursor.rownumber == 0

        next(instances_generator)
        assert cursor.rownumber == 1

        next(instances_generator)
        assert cursor.rownumber == 2

        assert not next(instances_generator, None)
        assert cursor.rownumber == 2


@pytest.mark.skipif(
    django.VERSION < (3, 1),
    reason=django_31_skip_reason,
)
def test_models_from_cursor_tolerates_additional_columns(
    mocked_model_foreign_keys, mocked_model_varying_fields
):
    with connection.cursor() as cursor:
        cursor.execute(
            f"ALTER TABLE {mocked_model_foreign_keys._meta.db_table} ADD COLUMN new_col text DEFAULT NULL"
        )
        cursor.execute(
            f"ALTER TABLE {mocked_model_varying_fields._meta.db_table} ADD COLUMN new_col text DEFAULT NULL"
        )

    instance = mocked_model_foreign_keys.objects.create(
        varying_fields=mocked_model_varying_fields.objects.create(
            title="test", updated_at=timezone.now()
        ),
        single_field=None,
    )

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            SELECT fk_t.*, vf_t.* FROM {mocked_model_foreign_keys._meta.db_table} fk_t
            INNER JOIN {mocked_model_varying_fields._meta.db_table} vf_t ON vf_t.id = fk_t.varying_fields_id
        """
        )

        queried_instances = list(
            models_from_cursor(
                mocked_model_foreign_keys,
                cursor,
                related_fields=["varying_fields"],
            )
        )

        assert len(queried_instances) == 1
        assert queried_instances[0].id == instance.id
        assert (
            queried_instances[0].varying_fields.id == instance.varying_fields.id
        )
