from orm.database import Table
from orm.fields import *


class User(Table):

    id = DefaultPrimaryKeyField()
    name = CharField(max_length=255, null=True)
    username = CharField(max_length=255, unique=True, verbose_name="Nickname")
    age = IntegerField()
    married = BooleanField()


User.migrate(dry_run=True)
breakpoint()
user = User(id=1, name="Charles", username="champagne", age=30)
breakpoint()
