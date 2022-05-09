import datetime

import pytest
from faker import Faker

from orm.database import Table
from orm.fields import (
    BooleanField,
    CharField,
    DefaultPrimaryKeyField,
    ForeignKeyField,
    IntegerField,
)
from orm.postgres import PostgreSQL

fake = Faker()


class TestUser(Table):

    id = DefaultPrimaryKeyField()
    name = CharField(max_length=255, null=True)
    username = CharField(max_length=255, unique=True, verbose_name="Nickname")
    sex = CharField(max_length=1)
    address = CharField(max_length=255)
    age = IntegerField()


class TestCompany(Table):

    id = DefaultPrimaryKeyField()
    owner = ForeignKeyField(TestUser)
    name = CharField(max_length=255)
    catch_phrase = CharField(max_length=255, null=True)
    active = BooleanField()


@pytest.fixture
def create_user():
    def inner(**kwargs):
        if "bulk_create" in kwargs:
            users = []
            for i in range(kwargs["bulk_create"]):
                data = fake.simple_profile()
                users.append(
                    dict(
                        name=kwargs.get("name", data["name"]),
                        username=kwargs.get("username", data["username"]),
                        sex=kwargs.get("sex", data["sex"]),
                        address=kwargs.get("address", data["address"]),
                        age=kwargs.get(
                            "age", datetime.datetime.now().year - data["birthdate"].year
                        ),
                    )
                )
            return TestUser.objects.bulk_create(users)
        data = fake.simple_profile()
        return TestUser.objects.create(
            name=kwargs.get("name", data["name"]),
            username=kwargs.get("username", data["username"]),
            sex=kwargs.get("sex", data["sex"]),
            address=kwargs.get("address", data["address"]),
            age=kwargs.get(
                "age", datetime.datetime.now().year - data["birthdate"].year
            ),
        )

    return inner


@pytest.fixture
def create_company():
    def inner(**kwargs):

        if "bulk_create" in kwargs:
            companies = []
            for i in range(kwargs["bulk_create"]):
                companies.append(
                    dict(
                        owner=kwargs.get("owner"),
                        name=kwargs.get("company", fake.company()),
                        catch_phrase=fake.catch_phrase(),
                        active=kwargs.get("active", fake.pybool()),
                    )
                )
            return TestCompany.objects.bulk_create(companies)
        return TestCompany.objects.create(
            owner=kwargs.get("owner"),
            name=kwargs.get("company", fake.company()),
            catch_phrase=fake.catch_phrase(),
            active=kwargs.get("active", fake.pybool()),
        )

    return inner


@pytest.fixture(autouse=True)
def migrate_db():
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='public'
    """
    with PostgreSQL() as pgsql:
        data = [x[0] for x in pgsql.fetch_query_results(query)]

        if "testuser" not in data:
            TestUser.migrate()
            print("Migration successful for TestUser !")

        if "testcompany" not in data:
            TestCompany.migrate()
            print("Migration successful for Company !")
    print("Finished Migration")


@pytest.fixture(autouse=True)
def reset_db():
    queries = [
        "DELETE FROM testcompany",
        "DELETE FROM testuser",
    ]
    with PostgreSQL() as pgsql:
        for query in queries:
            pgsql.query(query, [])
            pgsql.commit()
