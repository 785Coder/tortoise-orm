from __future__ import annotations

from types import ModuleType
from typing import cast

import pytest
from pypika_tortoise import Table
from pypika_tortoise.queries import Query

import tortoise.backends.dameng.client as dameng_client
from tortoise import Model, fields
from tortoise.backends.dameng.client import (
    DamengClient,
    _configure_dmpython_runtime_libraries,
    _normalise_model_identifiers,
    _split_script_statements,
)
from tortoise.backends.dameng.executor import DamengExecutor
from tortoise.exceptions import DBConnectionError


class FakeDmError(Exception):
    pass


class FakeDmIntegrityError(FakeDmError):
    pass


class FakeCursor:
    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.rowcount = 0
        self.description = [("ID",), ("NAME",)]
        self._last_query = ""

    def execute(self, query: str, values: list | None = None) -> None:
        self._last_query = query
        self.connection.executed.append((query, values))
        self.rowcount = 1

    def executemany(self, query: str, values: list) -> None:
        self.connection.executed_many.append((query, values))
        self.rowcount = len(values)

    def fetchall(self) -> list[tuple[int, str]]:
        if self._last_query.startswith("SELECT"):
            return [(1, "alpha")]
        raise FakeDmError("not a query")

    def close(self) -> None:
        self.connection.closed_cursors += 1


class FakeConnection:
    def __init__(self) -> None:
        self.executed: list[tuple[str, list | None]] = []
        self.executed_many: list[tuple[str, list]] = []
        self.closed_cursors = 0
        self.closed = False
        self.commits = 0
        self.rollbacks = 0
        self.autoCommit = True

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class FakeDmPython:
    Error = FakeDmError
    IntegrityError = FakeDmIntegrityError

    def __init__(self) -> None:
        self.connections: list[FakeConnection] = []
        self.connect_kwargs: list[dict] = []

    def connect(self, **kwargs) -> FakeConnection:
        self.connect_kwargs.append(kwargs)
        connection = FakeConnection()
        self.connections.append(connection)
        return connection


class FakeDb:
    def __init__(self) -> None:
        self.connection_name = "default"
        self.query_class = DamengClient.query_class
        self.scripts: list[str] = []
        self.inserts: list[tuple[str, list]] = []
        self.queries: list[str] = []

    async def execute_script(self, query: str) -> None:
        self.scripts.append(query)

    async def execute_insert(self, query: str, values: list) -> int:
        self.inserts.append((query, values))
        return 0

    async def execute_query_dict(
        self, query: str, values: list | None = None
    ) -> list[dict[str, int]]:
        self.queries.append(query)
        return [{"GLOBAL_IDENTITY": 42}]


def test_dameng_configures_bundled_windows_dll_directories(monkeypatch, tmp_path) -> None:
    module_dir = tmp_path / "site-packages"
    dmssl_dir = module_dir / "dmssl"
    dmssl_dir.mkdir(parents=True)
    module_file = module_dir / "dmPython.cp313-win_amd64.pyd"
    module_file.touch()

    class FakeDmPythonModule(FakeDmPython):
        __file__ = str(module_file)

    monkeypatch.setattr(dameng_client.sys, "platform", "win32")
    monkeypatch.setenv("PATH", "original")

    _configure_dmpython_runtime_libraries(cast(ModuleType, FakeDmPythonModule()))

    assert dameng_client.os.environ["PATH"].split(dameng_client.os.pathsep)[:1] == [
        str(dmssl_dir.resolve()),
    ]


def test_dameng_configures_linux_shared_library_directories(monkeypatch, tmp_path) -> None:
    module_dir = tmp_path / "site-packages"
    dmssl_dir = module_dir / "dmssl"
    lib_dir = module_dir / "lib"
    dmssl_dir.mkdir(parents=True)
    lib_dir.mkdir()
    module_file = module_dir / "dmPython.cpython-313-x86_64-linux-gnu.so"
    module_file.touch()
    dmssl_library = dmssl_dir / "libdmssl.so"
    dmssl_library.touch()
    (dmssl_dir / "libdmssl.so.1").touch()
    (dmssl_dir / "dmdpi").touch()
    (lib_dir / "libdmclient.so").touch()
    loaded_paths = []

    class FakeDmPythonModule(FakeDmPython):
        __file__ = str(module_file)

    monkeypatch.setattr(dameng_client.sys, "platform", "linux")
    monkeypatch.setenv("LD_LIBRARY_PATH", "original")
    monkeypatch.setattr(
        dameng_client.ctypes,
        "CDLL",
        lambda path, mode: loaded_paths.append((path, mode)),
    )

    _configure_dmpython_runtime_libraries(cast(ModuleType, FakeDmPythonModule()))

    assert dameng_client.os.environ["LD_LIBRARY_PATH"].split(dameng_client.os.pathsep)[:1] == [
        str(dmssl_dir.resolve()),
    ]
    assert loaded_paths == [(str(dmssl_library.resolve()), dameng_client.ctypes.RTLD_GLOBAL)]


@pytest.mark.asyncio
async def test_dameng_client_executes_via_dmpython(monkeypatch) -> None:
    fake_dm = FakeDmPython()
    monkeypatch.setattr(dameng_client, "_dmPython", fake_dm)

    client = DamengClient(
        connection_name="default",
        user="SYSDBA",
        password="SYSDBA001",
        host="127.0.0.1",
        port=5236,
        database="APP",
        minsize=2,
        maxsize=2,
    )
    await client.create_connection(with_db=True)

    assert fake_dm.connect_kwargs == [
        {
            "user": "SYSDBA",
            "password": "SYSDBA001",
            "server": "127.0.0.1",
            "port": 5236,
            "autoCommit": True,
        },
        {
            "user": "SYSDBA",
            "password": "SYSDBA001",
            "server": "127.0.0.1",
            "port": 5236,
            "autoCommit": True,
        },
    ]
    assert all(conn.executed[0] == ('SET SCHEMA "APP"', None) for conn in fake_dm.connections)

    rowcount, rows = await client.execute_query("SELECT ID, NAME FROM USERS WHERE ID=?", [1])
    assert rowcount == 1
    assert rows == [{"ID": 1, "NAME": "alpha"}]

    await client.execute_insert("INSERT INTO USERS (NAME) VALUES (?)", ["beta"])
    await client.execute_many("INSERT INTO USERS (NAME) VALUES (?)", [["a"], ["b"]])

    assert any(
        "INSERT INTO USERS" in query for conn in fake_dm.connections for query, _ in conn.executed
    )
    assert any(conn.executed_many for conn in fake_dm.connections)
    assert any(conn.commits == 1 for conn in fake_dm.connections)

    await client.close()
    assert all(conn.closed for conn in fake_dm.connections)


@pytest.mark.asyncio
async def test_dameng_client_reports_missing_dmpython(monkeypatch) -> None:
    monkeypatch.setattr(dameng_client, "_dmPython", None)
    monkeypatch.setattr(
        dameng_client,
        "_get_dmpython",
        lambda: (_ for _ in ()).throw(DBConnectionError("dmPython is required")),
    )

    client = DamengClient(
        connection_name="default",
        user="SYSDBA",
        password="SYSDBA001",
        host="127.0.0.1",
        port=5236,
    )

    with pytest.raises(DBConnectionError, match="dmPython is required"):
        await client.create_connection(with_db=True)


@pytest.mark.asyncio
async def test_dameng_client_wraps_chained_dmpython_connect_error(monkeypatch) -> None:
    class BrokenDmPython(FakeDmPython):
        def connect(self, **kwargs) -> FakeConnection:
            try:
                raise FakeDmError("[CODE:-70089]加密模块加载失败")
            except FakeDmError as exc:
                raise SystemError("dmPython.Connection failed") from exc

    monkeypatch.setattr(dameng_client, "_dmPython", BrokenDmPython())

    client = DamengClient(
        connection_name="default",
        user="SYSDBA",
        password="SYSDBA001",
        host="127.0.0.1",
        port=5236,
    )

    with pytest.raises(DBConnectionError, match="Can't connect to Dameng server"):
        await client.create_connection(with_db=True)


def test_dameng_script_splitter_ignores_semicolons_in_literals_and_comments() -> None:
    script = """
    INSERT INTO T (NAME) VALUES ('a;b');
    -- comment ; stays with the next statement separator
    INSERT INTO T (NAME) VALUES ('it''s ok');
    /* block ; comment */
    CREATE TABLE "semi;name" ("id" INT);
    """

    assert _split_script_statements(script) == [
        "INSERT INTO T (NAME) VALUES ('a;b')",
        "-- comment ; stays with the next statement separator\n"
        "    INSERT INTO T (NAME) VALUES ('it''s ok')",
        '/* block ; comment */\n    CREATE TABLE "semi;name" ("id" INT)',
    ]


def test_dameng_normalises_model_identifiers_to_uppercase() -> None:
    class Widget(Model):
        id = fields.IntField(pk=True)
        name = fields.CharField(max_length=32)

        class Meta:
            app = "models"
            table = "widget"

    _normalise_model_identifiers(Widget, DamengClient.query_class)

    assert Widget._meta.db_table == "WIDGET"
    assert Widget._meta.fields_db_projection == {"id": "ID", "name": "NAME"}
    assert Widget._meta.fields_db_projection_reverse == {"ID": "id", "NAME": "name"}
    assert Widget._meta.db_fields == {"ID", "NAME"}
    assert Widget._meta.db_pk_column == "ID"
    assert Widget._meta.generated_db_fields == ("ID",)
    basequery_sql = str(Widget._meta.basequery_all_fields)
    assert basequery_sql.startswith("SELECT ")
    assert '"ID"' in basequery_sql
    assert '"NAME"' in basequery_sql
    assert basequery_sql.endswith(' FROM "WIDGET"')


@pytest.mark.asyncio
async def test_dameng_executor_fetches_identity_from_global_identity() -> None:
    class Widget(Model):
        id = fields.IntField(pk=True)

        class Meta:
            app = "models"

    db = FakeDb()
    executor = object.__new__(DamengExecutor)
    executor.model = Widget
    executor.db = db  # type: ignore[assignment]
    instance = Widget()

    await executor._process_insert_result(instance, 0)

    assert db.queries == ["SELECT global_identity"]
    assert instance.pk == 42


@pytest.mark.asyncio
async def test_dameng_executor_wraps_custom_generated_pk_with_identity_insert() -> None:
    class Widget(Model):
        id = fields.IntField(pk=True)
        name = fields.CharField(max_length=32)

        class Meta:
            app = "models"
            table = "WIDGET"

    db = FakeDb()
    Widget._meta.db_table = "WIDGET"
    Widget._meta.fields_db_projection = {"id": "ID", "name": "NAME"}
    Widget._meta.finalise_fields()
    Widget._meta.basetable = Table(name=Widget._meta.db_table, schema=Widget._meta.schema)
    Widget._meta.basequery = cast(Query, db.query_class.from_(Widget._meta.basetable))
    executor = DamengExecutor(Widget, db)  # type: ignore[arg-type]
    instance = Widget(id=7, name="custom")

    await executor.execute_insert(instance)

    assert db.scripts == ["SET IDENTITY_INSERT WIDGET ON", "SET IDENTITY_INSERT WIDGET OFF"]
    assert db.inserts == [('INSERT INTO "WIDGET" ("ID","NAME") VALUES (?,?)', [7, "custom"])]
