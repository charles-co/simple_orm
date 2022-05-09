from orm.database import Table
from orm.fields import (
    BooleanField,
    CharField,
    DefaultPrimaryKeyField,
    ForeignKeyField,
    IntegerField,
)


class User(Table):

    id = DefaultPrimaryKeyField()
    name = CharField(max_length=255, null=True)
    username = CharField(max_length=255, unique=True, verbose_name="Nickname")
    sex = CharField(max_length=1)
    address = CharField(max_length=255)
    age = IntegerField()


class Company(Table):

    id = DefaultPrimaryKeyField()
    owner = ForeignKeyField(User)
    name = CharField(max_length=255)
    catch_phrase = CharField(max_length=255, null=True)
    active = BooleanField()


User.migrate()
Company.migrate()
