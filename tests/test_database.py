import unittest

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
