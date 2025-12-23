import unittest

import pandas as pd
import sqlalchemy as sa

from ibge_sidra_tabelas import database


class DummyConfig:
    def __init__(self, user, password, host, port, name, table, schema=None):
        self.db_user = user
        self.db_password = password
        self.db_host = host
        self.db_port = port
        self.db_name = name
        self.db_table = table
        self.db_schema = schema


class TestDatabaseHelpers(unittest.TestCase):
    def test_get_engine_fields(self):
        cfg = DummyConfig("u", "p", "h", 5432, "db", "t")
        eng = database.get_engine(cfg)
        # Engine URL object should expose the provided fields
        url = eng.url
        self.assertEqual(url.username, "u")
        self.assertEqual(url.password, "p")
        self.assertEqual(url.host, "h")
        self.assertEqual(url.port, 5432)
        self.assertEqual(url.database, "db")
        # drivername should contain postgresql
        self.assertIn("postgresql", url.drivername)

    def test_load_appends_to_table_using_sqlite(self):
        # Use an in-memory SQLite engine to test df.to_sql behavior
        engine = sa.create_engine("sqlite:///:memory:")
        cfg = DummyConfig("u", "p", "h", 0, "db", "my_table")

        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})

        # Should not raise
        database.load(df, engine, cfg)

        # Verify table exists and contents match
        result = pd.read_sql_table(cfg.db_table, con=engine)
        # SQLite will have inserted the two rows
        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result["a"]), [1, 2])

    def test_build_ddl_and_comment(self):
        cols = {"id": "BIGINT", "val": "TEXT"}
        ddl = database.build_ddl(
            "s", "t", "ts", cols, ["id"], comment="My table"
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS s.t", ddl)
        self.assertIn("CONSTRAINT t_pkey PRIMARY KEY (id)", ddl)
        self.assertIn("TABLESPACE ts;", ddl)
        self.assertIn("COMMENT ON TABLE s.t IS 'My table';", ddl)

    def test_build_dcl(self):
        dcl = database.build_dcl("s", "t", "owner_role", "reader_role")
        self.assertIn("ALTER TABLE IF EXISTS s.t OWNER TO owner_role;", dcl)
        self.assertIn("GRANT SELECT ON TABLE s.t TO reader_role;", dcl)


if __name__ == "__main__":
    unittest.main()
