import inspect
from collections import OrderedDict
from copy import deepcopy

from orm.commands import LOGICAL_SEPARATOR, SQL, Query
from orm.fields import BaseField, ForeignKeyField
from orm.postgres import PostgreSQL


class SQLException(Exception):
    pass


NON_COLUMN_FIELDS = ("__module__", "__doc__", "Meta", "_schema")


class QueryException(Exception):
    pass


class ObjectDoesNotExist(Exception):
    pass


class MultipleObjectsFound(Exception):
    pass


class RowSet:
    def __init__(self, table_class):
        self.pgsql = PostgreSQL()
        self.__table_class = table_class
        self.__table_columns = table_class.get_column_names()
        self.__filter_exclude_inputs = {
            "filter": {},
            "or_filter": {},
            "exclude": {},
            "order_by": {},
        }
        self.__limit = None
        self.__offset = None
        self.__delete = False
        self.__select_related = []
        self.__columns_order = []
        self.__value = None
        self.__table_details = None
        self.__base_table_proxy = None

    @staticmethod
    def get_details_from_table_proxy(proxy):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.pgsql.close()

    def __sql_read(self, query, params=()):
        for i in self.pgsql.fetch_query_results(query, params=params):
            yield i

    def __sql_delete(self, query, params=()):
        self.pgsql.query(query, params=params)
        self.pgsql.commit()

    def __update_query_inputs(self, data):
        if data:
            for k, v in data.items():
                self.__filter_exclude_inputs[k].update(v)

    def __create_query(self):
        query = Query(
            schema=self.__table_class.get_schema(),
            table_class=self.__table_class,
            table_columns=self.__table_columns,
            pk=self.__table_class.get_pk_name(),
            order_dict=self.__filter_exclude_inputs["order_by"],
            filter_dict=self.__filter_exclude_inputs["filter"],
            or_filter_dict=self.__filter_exclude_inputs["or_filter"],
            exclude_dict=self.__filter_exclude_inputs["exclude"],
            select_related=self.__select_related,
            limit=self.__limit,
            offset=self.__offset,
            delete=self.__delete,
        )
        (
            sql_query,
            params,
            column_query,
            self.__table_details,
            self.__base_table_proxy,
        ) = query.query()
        self.__columns_order = [i.strip().strip('n"') for i in column_query.split(",")]

        return {"query": sql_query, "params": params}

    def __set_attributes(self, column_values):

        data_map = {}
        used_proxies = []
        for i in range(len(column_values)):
            p, c = self.__columns_order[i].split(".")
            if p not in data_map:
                data_map[p] = {}
            data_map[p][c] = column_values[i]
        data_map = OrderedDict(
            sorted([(k, v) for k, v in data_map.items()], key=lambda x: x[0])
        )

        def get_table_proxy(tbl_class, base_tbl_class, f_key):

            for proxy_k, proxy_v in self.__table_details.items():
                conditions = (
                    proxy_v["details"]["fk_table_class"] == tbl_class
                    and proxy_v["details"]["base_table_class"] == base_tbl_class
                    and proxy_v["details"]["key"] == f_key
                )
                if conditions and proxy_k not in used_proxies:
                    used_proxies.append(proxy_k)
                    return proxy_k
            return

        def fill_table_attributes(proxy_name):

            if proxy_name == self.__base_table_proxy:
                table_class = self.__table_class
            else:
                table_class = self.__table_details[proxy_name]["details"][
                    "fk_table_class"
                ]
            obj = table_class()
            for column in table_class.get_column_names():
                if isinstance(table_class.__dict__[column], ForeignKeyField):
                    obj_fk_table_class = getattr(
                        table_class.__dict__[column], "table_name"
                    )
                    fk_proxy = get_table_proxy(
                        tbl_class=obj_fk_table_class,
                        base_tbl_class=table_class,
                        f_key=column,
                    )
                    if fk_proxy:
                        setattr(obj, column, fill_table_attributes(fk_proxy))
                    else:
                        obj_f_key = deepcopy(table_class.__dict__[column])
                        obj_f_key.set_value(data_map[proxy_name][column])
                        setattr(obj, column, obj_f_key)
                else:
                    setattr(obj, column, data_map[proxy_name][column])
            return obj

        main_obj = fill_table_attributes(proxy_name=self.__base_table_proxy)
        return main_obj

    def __getitem__(self, index):

        if isinstance(index, slice):
            if index.step:
                print(
                    "WARNING: step provided will be ignored. Not yet supported in PostgreSQL"
                )
            start = int(index.start) if index.start else 0
            if start < 0:
                raise ValueError("Start index cannot be negative.")
            self.__offset = start

            stop = int(index.stop) if index.stop else None
            if stop:
                if stop < start:
                    raise ValueError(
                        "Stop index cannot be negative and less than Start index."
                    )
                self.__limit = stop - start
            return self.__iter__()
        if isinstance(index, int):
            if index < 0:
                raise ValueError("Index cannot be negative.")
            self.__limit = 1
            self.__offset = index
            return [i for i in self.__iter__()][0]
        raise ValueError("Invalid index.")

    def __iter__(self):
        if self.__value is None:
            for i in self.__sql_read(**self.__create_query()):
                obj = self.__set_attributes(i)
                yield obj
        else:

            for i in self.__value:
                yield i

    def __next__(self):
        return next(self.__iter__())

    def __validate_kwargs(self, kwargs):
        for key in kwargs.keys():
            column_name = key.rsplit(LOGICAL_SEPARATOR, 1)[0]
            if column_name not in self.__table_columns:
                raise ValueError(
                    "The column could not be found. {}".format(column_name)
                )

    def create(self, **kwargs):
        obj = self.__table_class(**kwargs)
        obj.save(commit=True)
        return obj

    def bulk_create(self, obj_list):
        params = []
        base_table = "{}.{}".format(
            self.__table_class.get_schema(), self.__table_class.get_table_name()
        )
        column_names = [i for i in self.__table_class.get_column_names() if i != "id"]
        columns = ['"{}"'.format(i) for i in column_names if i != "id"]
        query = "INSERT INTO " + base_table + " (" + ", ".join(columns) + ") VALUES {};"
        for obj in obj_list:
            params.append(tuple([obj[k] for k in column_names]))
        self.pgsql.insert_many(query, params=params)
        self.pgsql.commit()

    def order_by(self, params):
        data = {}
        if type(params) == str:
            params = (params,)
        for i in params:
            order = "DESC" if i[0] == "-" else "ASC"
            column_name = i[1:] if i[0] == "-" else i
            if column_name not in self.__table_columns:
                raise QueryException("Column not found: {}".format(column_name))
            data[column_name] = order
        self.__update_query_inputs({"order_by": data})
        return self

    def all(self):
        self.__update_query_inputs({})
        return self

    def filter(self, **kwargs):
        self.__update_query_inputs({"filter": kwargs})
        return self

    def or_filter(self, **kwargs):
        self.__update_query_inputs({"or_filter": kwargs})
        return self

    def exclude(self, **kwargs):
        self.__update_query_inputs({"exclude": kwargs})
        return self

    def get(self, **kwargs):
        self.__filter_exclude_inputs["filter"] = kwargs
        objects_found = [i for i in self.__iter__()]
        if not objects_found:
            raise ObjectDoesNotExist("Object does not exist.")
        if len(objects_found) > 1:
            raise MultipleObjectsFound("Multiple objects found.")
        return objects_found[0]

    def delete(self):
        self.__delete = True
        self.__sql_delete(**self.__create_query())

    def count(self):
        self.__value = [i for i in self]
        return len(self.__value) if self.__value else 0


class Objects:
    def __get__(self, instance, owner):
        self.row_set = RowSet(
            table_class=owner,
        )
        return self.row_set


class Table:

    database_type = None

    _schema = "public"
    objects = Objects()

    def __init__(self, **kwargs):
        for i in self.__class__.get_column_names():
            self.__dict__[i] = kwargs.get(i)

    def __getattribute__(self, item: str):
        if item.startswith("__"):
            return object.__getattribute__(self, item)
        try:
            return self.__dict__[item]
        except KeyError:
            return object.__getattribute__(self, item)

    def __setattr__(self, key: str, value):
        if key.startswith("__"):
            object.__setattr__(self, key, value)
        try:
            self.__dict__[key] = value
        except KeyError:
            object.__setattr__(self, key, value)

    @classmethod
    def get_schema(cls):
        return cls._schema

    @classmethod
    def get_full_table_name(cls):
        return "{}.{}".format(cls.get_schema(), cls.get_table_name())

    @classmethod
    def __create_schema(cls):
        return SQL.create_schema().format(name=cls.get_schema())

    @classmethod
    def __create_table(cls):
        add_primary_key = ""
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                if not add_primary_key:
                    add_primary_key = "{name} {field_type} {properties}".format(
                        name=k, field_type=v.field_type, properties=v.properties
                    )
                else:
                    raise SQLException("Multiple primary keys found.")
        if not add_primary_key:
            raise SQLException("No primary key found.")
        return SQL.create_table().format(
            schema=cls.get_schema(),
            name=cls.get_table_name(),
            add_primary_key=add_primary_key,
        )

    @classmethod
    def _get_column_fields(cls):
        valid_column_names = cls.get_column_names()
        return {k: v for k, v in cls.__dict__.items() if k in valid_column_names}

    @classmethod
    def _get_meta_field(cls):
        return cls.__dict__.get("Meta")

    @classmethod
    def get_column_names(cls):
        names = []
        with PostgreSQL() as pgsql:
            pgsql.query(
                SQL.get_table_columns(), [cls.__name__.lower(), cls.get_schema()]
            )
            names = [name[0] for name in pgsql.fetchall()]

        if not names:
            for k, v in cls.__dict__.items():
                class_inspects = inspect.getmro(v.__class__)
                if len(class_inspects) > 1 and class_inspects[-2] == BaseField:
                    if k != k.lower():
                        raise SQLException("Column names should be in lowercase.")
                    names.append(k)
        return sorted(names)

    @classmethod
    def get_table_name(cls):
        return cls.__name__.lower()

    @classmethod
    def __create_columns(cls):
        schema = cls.get_schema()
        table_name = cls.get_table_name()
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                continue
            yield v.create(schema=schema, table_name=table_name, column_name=k)

    @classmethod
    def __create_meta_properties(cls):
        meta_queries = []
        meta_field = cls._get_meta_field()
        column_names = cls.get_column_names()
        if meta_field:
            unique_together = meta_field.__dict__.get("unique_together", ())
            for i in unique_together:
                for j in i:
                    if j not in column_names:
                        raise SQLException(
                            "Column {} does not exist in the model {}.".format(
                                j, cls.get_table_name()
                            )
                        )
                meta_queries.append(
                    SQL.add_unique_together().format(
                        schema=cls.get_schema(),
                        table_name=cls.get_table_name(),
                        constraint_name="{}_uniq".format("_".join(i)),
                        columns=",".join(['"{}"'.format(col for col in i)]),
                    )
                )
        return meta_queries

    @classmethod
    def get_value_or_object_pk(cls, value):
        return getattr(value, "pk") if hasattr(value, "pk") else value

    def __get_field_value(self, field_name):
        field_name = field_name.strip('"')
        value = getattr(self, field_name)
        return self.__class__.get_value_or_object_pk(value)

    @classmethod
    def migrate(cls, dry_run=False):
        queries = (
            ["BEGIN;", cls.__create_schema(), cls.__create_table()]
            + [i for i in cls.__create_columns()]
            + cls.__create_meta_properties()
            + ["COMMIT;"]
        )
        with PostgreSQL() as pgsql:
            for query in queries:
                if dry_run:
                    print(pgsql.mogrify(query))
                else:
                    pgsql.query(query)
                    pgsql.commit()

    @property
    def pk(self):
        for k, v in self.__class__._get_column_fields().items():
            if v.primary_key:
                return getattr(self, k)

    @classmethod
    def get_pk_name(cls):
        for k, v in cls._get_column_fields().items():
            if v.primary_key:
                return k

    def _sql_save(self, commit=True):
        if getattr(self, "pk"):
            pk_name = self.__class__.get_pk_name()
            column_names = [
                i for i in self.__class__.get_column_names() if i != pk_name
            ]
            params = [self.__get_field_value(i) for i in column_names] + [self.pk]
            query = SQL.update_table_row().format(
                schema=self.__class__.get_schema(),
                table_name=self.__class__.get_table_name(),
                set_key_value=", ".join(['"{}"=%s'.format(i) for i in column_names]),
                condition='"{}"=%s'.format(pk_name),
            )
            with PostgreSQL() as pgsql:
                pgsql.query(query, params=params)
                pgsql.commit()
        else:
            column_names = [
                '"{}"'.format(i) for i in self.__class__.get_column_names() if i != "id"
            ]
            params = [self.__get_field_value(i) for i in column_names]
            query = SQL.insert_table_row().format(
                schema=self.__class__.get_schema(),
                table_name=self.__class__.get_table_name(),
                column_names=", ".join(column_names),
                column_values=", ".join(["%s"] * len(column_names)),
            )
            obj_id = None
            if commit:
                with PostgreSQL() as pgsql:
                    pgsql.query(query, params=params)
                    obj_id = pgsql.fetchone()[0]
                    pgsql.commit()
            self.__dict__["id"] = obj_id

    def save(self, commit=True):
        self._sql_save(commit=commit)

    def delete(self):
        if getattr(self, "pk"):
            self.__class__.objects.filter(pk=self.pk).delete()
        else:
            raise SQLException("Missing primary key for the given object.")

    def as_dict(self):
        return {k: self.__dict__.get(k) for k in self.__class__.get_column_names()}
