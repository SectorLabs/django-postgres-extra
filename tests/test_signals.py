from unittest.mock import Mock

import pytest
from django.db import models
from django.test import TestCase

from psqlextra import signals

from .fake_model import get_fake_model


@pytest.mark.django_db
class SignalsTestCase(TestCase):
    """Tests whether all signals work correctly."""

    def mock_signal_handler(self, signal):
        """Creates a new model and attaches the specified
        signal to a mocked signal handler.

        Returns:
            The created model and the mocked signal handler.
        """

        model = get_fake_model({
            'title': models.CharField(max_length=255)
        })

        signal_handler = Mock()
        signal.connect(signal_handler, sender=model, weak=False)

        return model, signal_handler

    def test_create(self):
        """Tests whether the create signal is properly emitted
        when using QuerySet.create."""

        model, signal_handler = self.mock_signal_handler(signals.create)
        instance = model.objects.create(title='beer')

        assert signal_handler.call_count == 1
        assert signal_handler.call_args[1]['pk'] == instance.pk

    def test_model_save_create(self):
        """Tests whether the create signal is properly
        emitted when using Model.save()."""

        model, signal_handler = self.mock_signal_handler(signals.create)

        instance = model(title='beer')
        instance.save()

        assert signal_handler.call_count == 1
        assert signal_handler.call_args[1]['pk'] == instance.pk

    def test_model_save_update(self):
        """Tests whether the update signal properly
        emitted when using Model.save()."""

        model, signal_handler = self.mock_signal_handler(signals.update)

        instance = model(title='beer')
        instance.save()  # create
        instance.save()  # update

        assert signal_handler.call_count == 1
        assert signal_handler.call_args[1]['pk'] == instance.pk

    def test_model_delete(self):
        """Tests whether the delete signal properly
        emitted when using Model.delete()."""

        model, signal_handler = self.mock_signal_handler(signals.delete)
        instance = model.objects.create(title='beer')
        instance_pk = instance.pk
        instance.delete()

        assert signal_handler.call_count == 1
        assert signal_handler.call_args[1]['pk'] == instance_pk

    def test_query_set_delete(self):
        """Tests whether the delete signal is emitted
        for each row that is deleted."""

        model, signal_handler = self.mock_signal_handler(signals.delete)

        instance_1 = model.objects.create(title='beer')
        instance_1_pk = instance_1.pk
        instance_2 = model.objects.create(title='more boar')
        instance_2_pk = instance_2.pk

        model.objects.all().delete()

        assert signal_handler.call_count == 2
        assert signal_handler.call_args_list[0][1]['pk'] == instance_1_pk
        assert signal_handler.call_args_list[1][1]['pk'] == instance_2_pk

    def test_query_set_update(self):
        """Tests whether the update signal is emitted
        for each row that has been updated."""

        model, signal_handler = self.mock_signal_handler(signals.update)

        instance_1 = model.objects.create(title='beer')
        instance_2 = model.objects.create(title='more boar')

        model.objects.all().update(title='cookies')

        assert signal_handler.call_count == 2
        assert signal_handler.call_args_list[0][1]['pk'] == instance_1.pk
        assert signal_handler.call_args_list[1][1]['pk'] == instance_2.pk
