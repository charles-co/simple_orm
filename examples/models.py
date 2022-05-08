from orm.commands import Query
from orm.database import Table
from orm.fields import *


class User(Table):

    id = DefaultPrimaryKeyField()
    name = CharField(max_length=255, null=True)
    username = CharField(max_length=255, unique=True, verbose_name="Nickname")
    age = IntegerField()
    married = BooleanField(default=False)


Query("public", User, ["name"], "id").query()
breakpoint()
