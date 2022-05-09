import pytest
from psycopg2.errors import NotNullViolation

from .conftest import TestCompany, TestUser


class TestORM:
    def test_bulk_create(self):
        TestUser.objects.bulk_create(
            [
                dict(
                    name="Charles",
                    username="charles",
                    sex="M",
                    address="Lagos, Nigeria",
                    age=40,
                ),
                dict(
                    name="Sunshine",
                    username="sunshine",
                    sex="F",
                    address="London, UK",
                    age=26,
                ),
            ]
        )
        assert TestUser.objects.all().count() == 2

    def test_user_creation(self, create_user):
        create_user(bulk_create=2)
        assert TestUser.objects.all().count() == 2

    def test_company_creation(self, create_company, create_user):
        user = create_user()
        create_company(owner=user.id, bulk_create=2)
        assert TestCompany.objects.all().count() == 2

    def test_company_user_not_null(self, create_company):
        with pytest.raises(NotNullViolation):
            create_company()
            raise NotNullViolation()

    def test_username_not_null(self, create_user):
        with pytest.raises(NotNullViolation):
            create_user(username=None)
            raise NotNullViolation()

    def test_filter(self, create_user):
        create_user(bulk_create=9)
        create_user(name="Charles Oraegbu")

        assert TestUser.objects.all().count() == 10
        assert TestUser.objects.filter(name="Charles Oraegbu").count() == 1

    def test_or_filter(self, create_user):
        create_user(bulk_create=9)
        create_user(name="Charleset Oraegbu")
        create_user(name="Michael Owenist")

        assert TestUser.objects.all().count() == 11
        assert TestUser.objects.or_filter(
            name__startswith="Charleset", name__endswith="Owenist"
        )

    def test_delete(self, create_user):
        create_user(bulk_create=9)
        users = TestUser.objects.all()
        users[0].delete()
        assert TestUser.objects.all().count() == 8

    def test_refiltering(self, create_user):
        create_user(name="Charlesxet Oraegbu", age=20)
        create_user(name="Charlesx Oraegbu", age=10)
        create_user(name="Darwin Oraegbu", age=10)
        create_user(bulk_create=9)
        users = TestUser.objects.filter(name__contains="Charlesx")
        first_filter = [x for x in users]
        users.filter(age=10)
        second_filter = [x for x in users]
        assert TestUser.objects.all().count() == 12
        assert len(first_filter) == 2
        assert len(second_filter) == 1

    def test_filter_by_fkey(self, create_company, create_user):

        user = create_user()
        user1 = create_user()
        create_company(owner=user.id, bulk_create=2)
        create_company(owner=user1.id, bulk_create=5)

        assert TestCompany.objects.filter(owner__id=user.id).count() == 2

    def test_update(self, create_user):

        user = create_user()
        user.name = "Charles Oraegbu"
        user.save()
        assert TestUser.objects.filter(name="Charles Oraegbu").count() == 1
