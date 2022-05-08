from collections import OrderedDict


class InvalidQueryException(Exception):
    pass


class SQL:
    @staticmethod
    def create_schema():
        return "CREATE SCHEMA IF NOT EXISTS {name};"

    @staticmethod
    def create_table():
        return "CREATE TABLE IF NOT EXISTS {schema}.{name} ({add_primary_key});"

    @staticmethod
    def add_table_column():
        return 'ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS "{name}" {field_type} {properties};'

    @staticmethod
    def add_unique_together():
        return "ALTER TABLE {schema}.{table_name} ADD CONSTRAINT {constraint_name} UNIQUE ({columns});"

    @staticmethod
    def insert_table_row():
        return "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ({column_values}) RETURNING id;"

    @staticmethod
    def update_table_row():
        return "UPDATE {schema}.{table_name} SET {set_key_value} WHERE {condition};"


LOGICAL_SEPARATOR = "__"


class Query:
    def __init__(
        self,
        schema,
        table_class,
        table_columns,
        pk,
        order_dict=None,
        filter_dict=None,
        or_filter_dict=None,
        exclude_dict=None,
        delete=False,
    ):
        breakpoint()
        self.__schema = schema
        self.__table_class = table_class
        self.__table_name = table_class.get_table_name()
        self.__full_table_name = table_class.get_full_table_name()
        self.__table_columns = table_columns
        self.__pk = pk

        self.__order_dict = order_dict if order_dict else {"id": "ASC"}
        self.__filter_dict = filter_dict if filter_dict else {}
        self.__or_filter_dict = or_filter_dict if or_filter_dict else {}
        self.__exclude_dict = exclude_dict if exclude_dict else {}
        self.__delete = delete
        self.__operators = {
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "exact": "=",
            "iexact": "=",
            "contains": "LIKE",
            "icontains": "ILIKE",
            "startswith": "LIKE",
            "istartswith": "ILIKE",
            "endswith": "LIKE",
            "iendswith": "ILIKE",
            "isnull": "IS",
            "in": "=",
        }
        self.__params = []
        self.__base_query = "SELECT {};" if not delete else "DELETE {};"
        self.__from_query = ""
        self.__where_query = ""
        self.__order_by_query = ""
        self.__column_query = ""
        self.__proxy_name_count = -1
        self.__base_table_proxy = self.generate_table_name_proxy()
        self.__table_details = OrderedDict()

    def generate_table_name_proxy(self):
        self.__proxy_name_count += 1
        return "table_{}".format(self.__proxy_name_count)

    def __create_where_query(self):
        breakpoint()
        filter_query = ""
        if self.__or_filter_dict:
            filter_query = "( {} )".format(
                " OR ".join(
                    [
                        self.__change_to_sql_conditions(k, v)
                        for k, v in self.__or_filter_dict.items()
                    ]
                )
            )
        if self.__filter_dict:
            if filter_query:
                filter_query += " AND "
            filter_query += " AND ".join(
                [
                    self.__change_to_sql_conditions(k, v)
                    for k, v in self.__filter_dict.items()
                ]
            )
        if self.__exclude_dict:
            if filter_query:
                filter_query += " AND "
            filter_query += "NOT "
            filter_query += " AND NOT ".join(
                [
                    self.__change_to_sql_conditions(k, v)
                    for k, v in self.__exclude_dict.items()
                ]
            )
        if filter_query:
            filter_query = " WHERE {}".format(filter_query)
        self.__where_query = filter_query

    def __create_from_query(self):
        breakpoint()
        proxy_name = self.__base_table_proxy
        if self.__delete:
            self.__from_query = "FROM {} AS {}".format(
                self.__full_table_name, proxy_name
            )
        else:
            self.__column_query = ", ".join(
                ["{}.{}".format(proxy_name, i) for i in self.__table_columns]
            )
            self.__from_query = "{} FROM {}".format(
                self.__column_query,
                "{} AS {}".format(self.__full_table_name, proxy_name),
            )

    def __logical_conditions(self, key, value, condition, table_proxy_name=None):
        breakpoint()
        if not table_proxy_name:
            table_proxy_name = self.__base_table_proxy
        if key == "pk":
            key = self.__pk
        key = "{}.{}".format(table_proxy_name, key)
        if condition == "=":
            self.__params.append(value)
            return "{key}=%s".format(key=key)
        if condition == "in":
            if isinstance(value, list):
                self.__params.append(value)
                return "{key} {operation} ANY(%s)".format(
                    key=key, operation=self.__operators[condition]
                )
            raise InvalidQueryException("Value should be a list.")
        if condition == "isnull":
            base = "{key} {operation}".format(
                key=key, operation=self.__operators[condition]
            )
            return "{} NULL".format(base) if value else "{} NOT NULL".format(base)
        if condition == "iexact":
            self.__params.append(value)
            return "LOWER({key}){operation}LOWER(%s)".format(
                key=key,
                operation=self.__operators[condition],
            )
        if condition in (
            "contains",
            "icontains",
            "startswith",
            "istartswith",
            "endswith",
            "iendswith",
        ):
            if condition == "contains" or condition == "icontains":
                self.__params.append("%{}%".format(value))
            elif condition == "startswith" or condition == "istartswith":
                self.__params.append("{}%".format(value))
            elif condition == "endswith" or condition == "iendswith":
                self.__params.append("%{}".format(value))
            return "{key} {operation} %s".format(
                key=key,
                operation=self.__operators[condition],
            )
        self.__params.append(value)

        return "{key}{operation}%s".format(
            key=key,
            operation=self.__operators[condition],
        )

    def __change_to_sql_conditions(self, key, value):
        breakpoint()
        # TODO: Tasks pending completion -@charles-PC at 5/7/2022, 6:52:12 PM
        # Add Foreign key field condition

        key_splits = key.split(LOGICAL_SEPARATOR)
        return self.__logical_conditions(key=key_splits[0], value=value, condition="=")

    def __create_order_by_query(self):
        order_query = ""
        if not self.__delete:
            if self.__order_dict:
                order_query = ", ".join(
                    [
                        "{}.{} {}".format(self.__base_table_proxy, k, v)
                        for k, v in self.__order_dict.items()
                    ]
                )
            if order_query:
                order_query = " ORDER BY {}".format(order_query)
        self.__order_by_query = order_query

    def query(self):

        # NOTE: Needs discussion or investigation -@charles-PC at 5/7/2022, 7:04:29 PM
        # Following stand SQL statements SELECT, FROM, WHERE, ORDER BY, LIMIT etc ...
        self.__create_from_query()
        self.__create_where_query()
        self.__create_order_by_query()

        query = self.__from_query + self.__where_query + self.__order_by_query
        breakpoint()
        return (
            self.__base_query.format(query),
            self.__params,
            self.__column_query,
            self.__table_details,
            self.__base_table_proxy,
        )
