import os

import psycopg2


class PostgreSQL:
    def __init__(self):
        credentials = {
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
        }
        try:
            self._conn = psycopg2.connect(**credentials)
            self._cursor = self._conn.cursor()
            print("\nConnected to PostgreSQL\n")
        except psycopg2.Error as error:
            raise ValueError(
                "Unable to connect to PostgreSQL database\n{error}".format(error=error)
            )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def __del__(self):
        self.close()

    def close(self):
        self.connection.close()
        print("\nClosed PostgreSQL connection.\n")

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def query(self, sql, params=None):
        print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())

    def mogrify(self, sql, params=None):
        return self.cursor.mogrify(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def insert(self, sql, params=None):
        print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())
        self.commit()

    def insert_many(self, sql, params=None):
        params_str = ",".join(
            (self.mogrify("%s", (x,))).decode("utf-8") for x in params
        )
        print(self.mogrify(sql.format(params_str)))
        self.cursor.execute(sql.format(params_str))
        self.commit()

    def fetch_query_results(self, sql, params=None):
        print(self.mogrify(sql, params))
        self.cursor.execute(sql, params or ())
        while True:
            try:
                results = self.cursor.fetchmany(100)
                if not results:
                    break
                for result in results:
                    yield result
            except psycopg2.ProgrammingError:
                break
