from django.db import models

from .fake_model import get_fake_model


def test_invalues_lookup_text_field():
    model = get_fake_model({"name": models.TextField()})
    [a, b] = model.objects.bulk_create(
        [
            model(name="a"),
            model(name="b"),
        ]
    )

    results = list(model.objects.filter(name__invalues=[a.name, b.name, "c"]))
    assert results == [a, b]


def test_invalues_lookup_integer_field():
    model = get_fake_model({"number": models.IntegerField()})
    [a, b] = model.objects.bulk_create(
        [
            model(number=1),
            model(number=2),
        ]
    )

    results = list(
        model.objects.filter(number__invalues=[a.number, b.number, 3])
    )
    assert results == [a, b]


def test_invalues_lookup_uuid_field():
    model = get_fake_model({"value": models.UUIDField()})
    [a, b] = model.objects.bulk_create(
        [
            model(value="f8fe0431-29f8-4c4c-839c-8a6bf29f95d5"),
            model(value="2fb0f45b-afaf-4e24-8637-2d81ded997bb"),
        ]
    )

    results = list(
        model.objects.filter(
            value__invalues=[
                a.value,
                b.value,
                "d7a8df83-f3f8-487b-b982-547c8f22b0bb",
            ]
        )
    )
    assert results == [a, b]


def test_invalues_lookup_related_field():
    model_1 = get_fake_model({"name": models.TextField()})
    model_2 = get_fake_model(
        {"relation": models.ForeignKey(model_1, on_delete=models.CASCADE)}
    )

    [a_relation, b_relation] = model_1.objects.bulk_create(
        [
            model_1(name="a"),
            model_1(name="b"),
        ]
    )

    [a, b] = model_2.objects.bulk_create(
        [model_2(relation=a_relation), model_2(relation=b_relation)]
    )

    results = list(
        model_2.objects.filter(relation__invalues=[a_relation, b_relation])
    )
    assert results == [a, b]


def test_invalues_lookup_related_field_subquery():
    model_1 = get_fake_model({"name": models.TextField()})
    model_2 = get_fake_model(
        {"relation": models.ForeignKey(model_1, on_delete=models.CASCADE)}
    )

    [a_relation, b_relation] = model_1.objects.bulk_create(
        [
            model_1(name="a"),
            model_1(name="b"),
        ]
    )

    [a, b] = model_2.objects.bulk_create(
        [model_2(relation=a_relation), model_2(relation=b_relation)]
    )

    results = list(
        model_2.objects.filter(
            relation__invalues=model_1.objects.all().values_list(
                "id", flat=True
            )
        )
    )
    assert results == [a, b]
