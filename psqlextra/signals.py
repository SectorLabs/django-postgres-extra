from django.dispatch import Signal

create = Signal(providing_args=['pk'])
update = Signal(providing_args=['pk'])
delete = Signal(providing_args=['pk'])
