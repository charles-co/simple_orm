from .conftest import TestCompany, TestUser


class TestORM:
    def test_user_creation(self, create_user):
        create_user(bulk_create=2)
        assert TestUser.objects.all().count() == 2

    def test_company_creation(self, create_company, create_user):
        user = create_user()
        create_company(owner=user.id, bulk_create=2)
        assert TestCompany.objects.all().count() == 2

    # def test_company_user_not_null(self, create_company, create_user)
