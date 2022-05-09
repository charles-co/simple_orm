from orm.commands import Query
from orm.database import Table
from orm.fields import *


class User(Table):

    id = DefaultPrimaryKeyField()
    name = CharField(max_length=255, null=True)
    username = CharField(max_length=255, unique=True, verbose_name="Nickname")
    age = IntegerField()
    married = BooleanField(default=False)


class Car(Table):

    id = DefaultPrimaryKeyField()
    owner = ForeignKeyField(User)
    name = CharField(max_length=255, null=True)
    automatic = BooleanField(default=False)


car = Car.objects.filter(owner__name="onoze")
car[0].name
print("-" * 50)
car[0].owner
print("-" * 50)


car[0].owner.username
print("-" * 50)

breakpoint()
