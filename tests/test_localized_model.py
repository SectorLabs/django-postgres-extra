from django.test import TestCase

from postgres_extra import LocalizedField, LocalizedValue

from .fake_model import get_fake_model


class LocalizedModelTestCase(TestCase):
    """Tests whether the :see:LocalizedModel class."""

    TestModel = None

    @classmethod
    def setUpClass(cls):
        """Creates the test model in the database."""

        super(LocalizedModelTestCase, cls).setUpClass()

        cls.TestModel = get_fake_model(
            'LocalizedModelTestCase',
            {
                'title': LocalizedField()
            }
        )

    @classmethod
    def test_defaults(cls):
        """Tests whether all :see:LocalizedField
        fields are assigned an empty :see:LocalizedValue
        instance when the model is instanitiated."""

        obj = cls.TestModel()

        assert isinstance(obj.title, LocalizedValue)


    @classmethod
    def test_model_init_kwargs(cls):
        """Tests whether all :see:LocalizedField
        fields are assigned an empty :see:LocalizedValue
        instance when the model is instanitiated."""
        data = {
            'title': {
                'en': 'english_title',
                'ro': 'romanian_title',
                'nl': 'dutch_title'
            }
        }
        obj = cls.TestModel(**data)

        assert isinstance(obj.title, LocalizedValue)
        assert obj.title.en == 'english_title'
        assert obj.title.ro == 'romanian_title'
        assert obj.title.nl == 'dutch_title'
