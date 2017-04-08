Django has long supported "signals", a feature that can be really useful. It allows you to get notified in a callback when a certain event occurs. One of the most common use cases is connecting to model signals. These signals get triggered when a model gets saved, or deleted.

Django's built-in signals have one major flaw:

* The `QuerySet.update(..)` method does **not** emit **any** signals.

Because of this limitation, Django's signals cannot be reliably used to be signaled on model changes. `django-postges-extra` adds three new signals which are much more primitive, but work reliably across the board.

The signals defined by this library are completely valid, standard Django signals. Therefore, their documentation also applies: [https://docs.djangoproject.com/en/1.10/topics/signals/](https://docs.djangoproject.com/en/1.10/topics/signals/).

Each of the signals send upon model modification send one parameter containing the value of the primary key of the row that was affected. Therefore the signal's signature looks like this:

    def my_receiver(sender, pk: int):
        # pk is the primary key, a keyword argument

## psqlextra.signals.create
Send **after** a new model instance was created.

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra import signals

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    def on_create(sender, **kwargs):
         print('model created with pk %d' % kwargs['pk'])

    signals.create.connect(MyModel, on_create, weak=False)

    # this will trigger the signal
    instance = MyModel(myfield='cookies')
    instance.save()

    # but so will this
    MyModel.objects.create(myfield='cheers')

## psqlextra.signals.update
Send **after** a new model instance was updated.

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra import signals

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    def on_update(sender, **kwargs):
         print('model updated with pk %d' % kwargs['pk'])

    signals.update.connect(MyModel, on_update, weak=False)

    # for every row that is affected, the signal will be send
    MyModel.objects.filter(myfield='cookies').update(myfield='cheers')

## psqlextra.signals.delete
Send **before** a new model instance is deleted.

    from django.db import models
    from psqlextra.models import PostgresModel
    from psqlextra import signals

    class MyModel(PostgresModel):
        myfield = models.CharField(max_length=255, unique=True)

    def on_delete(sender, **kwargs):
         print('model deleted with pk %d' % kwargs['pk'])

    signals.delete.connect(MyModel, on_update, weak=False)

    # for every row that is affected, the signal will be send
    MyModel.objects.filter(myfield='cookies').delete()

    # in this case, a single row is deleted, the signal will be send
    # for this particular row
    MyModel.objects.get(id=1).delete()
