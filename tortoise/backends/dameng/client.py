from __future__ import annotations

import asyncio
import ctypes
import os
import sys
from collections.abc import Callable, Coroutine
from functools import wraps
from itertools import count
from pathlib import Path
from types import ModuleType
from typing import Any, SupportsInt, TypeVar

from pypika_tortoise import OracleQuery

from tortoise.backends.base.client import (
    BaseDBAsyncClient,
    Capabilities,
    ConnectionWrapper,
    NestedTransactionContext,
    PoolConnectionWrapper,
    TransactionalDBClient,
    TransactionContext,
    TransactionContextPooled,
)
from tortoise.backends.dameng.executor import DamengExecutor
from tortoise.backends.dameng.schema_generator import DamengSchemaGenerator
from tortoise.exceptions import (
    DBConnectionError,
    IntegrityError,
    OperationalError,
    TransactionManagementError,
)

T = TypeVar("T")
FuncType = Callable[..., Coroutine[None, None, T]]
_dmPython: ModuleType | None = None


def _get_dmpython() -> ModuleType:
    global _dmPython
    if _dmPython is None:
        try:
            import dmPython
        except ImportError as exc:
            raise DBConnectionError(
                "dmPython is required for the Dameng backend. "
                'Install it with `pip install "tortoise-orm[dameng]"`.'
            ) from exc
        _configure_dmpython_runtime_libraries(dmPython)
        _dmPython = dmPython
    return _dmPython


def _configure_dmpython_runtime_libraries(dm_python: ModuleType) -> None:
    module_file = getattr(dm_python, "__file__", None)
    if not module_file:
        return
    module_dir = Path(module_file).resolve().parent
    dmssl_dir = module_dir / "dmssl"
    if not dmssl_dir.is_dir():
        return

    env_name = "PATH" if sys.platform.startswith("win") else "LD_LIBRARY_PATH"
    _prepend_env_path(env_name, dmssl_dir)

    if not sys.platform.startswith("win"):
        for library in dmssl_dir.glob("*.so"):
            try:
                ctypes.CDLL(str(library), mode=ctypes.RTLD_GLOBAL)
            except OSError:
                continue


def _prepend_env_path(name: str, path: Path) -> None:
    resolved = str(path)
    paths = [item for item in os.environ.get(name, "").split(os.pathsep) if item]
    if resolved in paths:
        return
    os.environ[name] = os.pathsep.join([resolved, *paths])


def _is_dmpython_error(exc: BaseException) -> bool:
    if _dmPython is None:
        return False
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, _dmPython.Error):
            return True
        current = current.__cause__ or current.__context__
    return False


def _is_dmpython_integrity_error(exc: BaseException) -> bool:
    if _dmPython is None:
        return False
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, _dmPython.IntegrityError):
            return True
        current = current.__cause__ or current.__context__
    return False


def translate_exceptions(func: FuncType) -> FuncType:
    @wraps(func)
    async def translate_exceptions_(self, *args) -> T:
        try:
            return await func(self, *args)
        except Exception as exc:
            if _is_dmpython_integrity_error(exc):
                raise IntegrityError(exc)
            if _is_dmpython_error(exc):
                raise OperationalError(exc)
            raise

    return translate_exceptions_


class DamengPool:
    def __init__(
        self,
        minsize: int,
        maxsize: int,
        connect_kwargs: dict[str, Any],
        schema: str | None = None,
    ) -> None:
        self.minsize = minsize
        self.maxsize = maxsize
        self.connect_kwargs = connect_kwargs
        self.schema = schema
        self._queue: asyncio.Queue = asyncio.Queue(maxsize)
        self._connections: list[Any] = []
        self._closed = False
        self._lock = asyncio.Lock()

    async def init(self) -> None:
        for _ in range(self.minsize):
            connection = await self._connect()
            self._connections.append(connection)
            await self._queue.put(connection)

    async def _connect(self) -> Any:
        dm_python = _get_dmpython()
        connection = await asyncio.to_thread(dm_python.connect, **self.connect_kwargs)
        if self.schema:
            await asyncio.to_thread(_execute, connection, f"SET SCHEMA {_quote(self.schema)}", None)
        return connection

    async def acquire(self) -> Any:
        if self._closed:
            raise DBConnectionError("Dameng connection pool is closed")
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            async with self._lock:
                if len(self._connections) < self.maxsize:
                    connection = await self._connect()
                    self._connections.append(connection)
                    return connection
            return await self._queue.get()

    async def release(self, connection: Any) -> None:
        if self._closed:
            await asyncio.to_thread(connection.close)
            return
        await self._queue.put(connection)

    async def close(self) -> None:
        self._closed = True
        while not self._queue.empty():
            self._queue.get_nowait()
        for connection in self._connections:
            await asyncio.to_thread(connection.close)
        self._connections.clear()


class DamengClient(BaseDBAsyncClient):
    query_class = OracleQuery
    schema_generator = DamengSchemaGenerator
    executor_class = DamengExecutor
    capabilities = Capabilities("dameng")

    def __init__(
        self,
        *,
        user: str,
        password: str,
        host: str,
        port: SupportsInt,
        database: str | None = None,
        schema: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.user = user
        self.password = password
        self.host = host
        self.port = int(port)
        self.database = database
        self.schema = schema or database
        self.extra = kwargs.copy()
        self.extra.pop("connection_name", None)
        self.extra.pop("fetch_inserted", None)
        self.minsize = int(self.extra.pop("minsize", 1))
        self.maxsize = int(self.extra.pop("maxsize", 5))
        self.echo = self.extra.pop("echo", False)
        self._template: dict[str, Any] = {}
        self._pool: DamengPool | None = None
        self._connection = None
        self._pool_init_lock = asyncio.Lock()

    async def create_connection(self, with_db: bool) -> None:
        self._template = {
            "user": self.user,
            "password": self.password,
            "server": self.host,
            "port": self.port,
            "autoCommit": True,
            **self.extra,
        }
        try:
            schema = self.schema if with_db else None
            self._pool = DamengPool(self.minsize, self.maxsize, self._template, schema)
            await self._pool.init()
            await self._post_connect()
            self.log.debug("Created Dameng connection pool with params: %s", self._template)
        except Exception as exc:
            if _is_dmpython_error(exc):
                raise DBConnectionError(
                    f"Can't connect to Dameng server: {self._template}"
                ) from exc
            raise

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self.log.debug("Closed Dameng connection pool with params: %s", self._template)
            self._pool = None
        self._template.clear()

    async def db_create(self) -> None:
        if not self.database:
            return
        await self.create_connection(with_db=False)
        await self.execute_script(f"CREATE SCHEMA {_quote(self.database)}")
        await self.close()

    async def db_delete(self) -> None:
        if not self.database:
            return
        await self.create_connection(with_db=False)
        try:
            await self.execute_script(f"DROP SCHEMA {_quote(self.database)} CASCADE")
        except OperationalError as exc:
            if "not exist" not in str(exc).lower() and "does not exist" not in str(exc).lower():
                raise
        await self.close()

    def acquire_connection(self) -> ConnectionWrapper | PoolConnectionWrapper:
        return PoolConnectionWrapper(self, self._pool_init_lock)

    def _in_transaction(self) -> TransactionContext:
        return TransactionContextPooled(TransactionWrapper(self), self._pool_init_lock)

    @translate_exceptions
    async def execute_insert(self, query: str, values: list) -> int:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            await asyncio.to_thread(_execute, connection, query, values)
            return 0

    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            await asyncio.to_thread(_executemany, connection, query, values)

    @translate_exceptions
    async def execute_query(self, query: str, values: list | None = None) -> tuple[int, list[dict]]:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            return await asyncio.to_thread(_execute_query, connection, query, values)

    async def execute_query_dict(self, query: str, values: list | None = None) -> list[dict]:
        return (await self.execute_query(query, values))[1]

    @translate_exceptions
    async def execute_script(self, query: str) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug(query)
            await asyncio.to_thread(_execute_script, connection, query)


class TransactionWrapper(DamengClient, TransactionalDBClient):
    def __init__(self, connection: DamengClient) -> None:
        self.connection_name = connection.connection_name
        self._connection = connection._connection
        self._lock = asyncio.Lock()
        self._savepoint: str | None = None
        self.log = connection.log
        self._finalized = False
        self.fetch_inserted = connection.fetch_inserted
        self._parent = connection

    def _in_transaction(self) -> TransactionContext:
        return NestedTransactionContext(TransactionWrapper(self))

    def acquire_connection(self) -> ConnectionWrapper:
        return ConnectionWrapper(self._lock, self)

    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            self.log.debug("%s: %s", query, values)
            await asyncio.to_thread(_executemany_no_commit, connection, query, values)

    async def begin(self) -> None:
        self._finalized = False
        await asyncio.to_thread(setattr, self._connection, "autoCommit", False)

    async def commit(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await asyncio.to_thread(self._connection.commit)
        self._finalized = True
        await asyncio.to_thread(setattr, self._connection, "autoCommit", True)

    async def rollback(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        await asyncio.to_thread(self._connection.rollback)
        self._finalized = True
        await asyncio.to_thread(setattr, self._connection, "autoCommit", True)

    async def savepoint(self) -> None:
        self._savepoint = _gen_savepoint_name()
        await asyncio.to_thread(_execute, self._connection, f"SAVEPOINT {self._savepoint}", None)

    async def savepoint_rollback(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        if self._savepoint is None:
            raise TransactionManagementError("No savepoint to rollback to")
        await asyncio.to_thread(
            _execute, self._connection, f"ROLLBACK TO SAVEPOINT {self._savepoint}", None
        )
        self._savepoint = None
        self._finalized = True

    async def release_savepoint(self) -> None:
        if self._finalized:
            raise TransactionManagementError("Transaction already finalised")
        if self._savepoint is None:
            raise TransactionManagementError("No savepoint to release")
        await asyncio.to_thread(_execute, self._connection, f"RELEASE SAVEPOINT {self._savepoint}", None)
        self._savepoint = None
        self._finalized = True


def _execute(connection: Any, query: str, values: list | None) -> int:
    cursor = connection.cursor()
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        return cursor.rowcount
    finally:
        cursor.close()


def _executemany(connection: Any, query: str, values: list) -> None:
    try:
        _executemany_no_commit(connection, query, values)
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


def _executemany_no_commit(connection: Any, query: str, values: list) -> None:
    cursor = connection.cursor()
    try:
        cursor.executemany(query, values)
    finally:
        cursor.close()


def _execute_query(connection: Any, query: str, values: list | None) -> tuple[int, list[dict]]:
    cursor = connection.cursor()
    try:
        if values:
            cursor.execute(query, values)
        else:
            cursor.execute(query)
        try:
            rows = cursor.fetchall()
        except Exception as exc:
            if _dmPython is None or not isinstance(exc, _dmPython.Error):
                raise
            return cursor.rowcount, []
        if not rows:
            return cursor.rowcount, []
        fields = [c[0] for c in cursor.description]
        return cursor.rowcount, [dict(zip(fields, row)) for row in rows]
    finally:
        cursor.close()


def _execute_script(connection: Any, query: str) -> None:
    cursor = connection.cursor()
    try:
        for statement in _split_script_statements(query):
            if statement.strip():
                cursor.execute(statement)
    finally:
        cursor.close()


def _split_script_statements(script: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    length = len(script)

    while i < length:
        char = script[i]
        next_char = script[i + 1] if i + 1 < length else ""

        if in_line_comment:
            current.append(char)
            if char in "\r\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            current.append(char)
            if char == "*" and next_char == "/":
                current.append(next_char)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if in_single_quote:
            current.append(char)
            if char == "'" and next_char == "'":
                current.append(next_char)
                i += 2
                continue
            if char == "'":
                in_single_quote = False
            i += 1
            continue

        if in_double_quote:
            current.append(char)
            if char == '"' and next_char == '"':
                current.append(next_char)
                i += 2
                continue
            if char == '"':
                in_double_quote = False
            i += 1
            continue

        if char == "-" and next_char == "-":
            current.extend((char, next_char))
            in_line_comment = True
            i += 2
            continue
        if char == "/" and next_char == "*":
            current.extend((char, next_char))
            in_block_comment = True
            i += 2
            continue
        if char == "'":
            current.append(char)
            in_single_quote = True
            i += 1
            continue
        if char == '"':
            current.append(char)
            in_double_quote = True
            i += 1
            continue
        if char == ";":
            if statement := "".join(current).strip():
                statements.append(statement)
            current.clear()
            i += 1
            continue

        current.append(char)
        i += 1

    if current and (statement := "".join(current).strip()):
        statements.append(statement)
    return statements


def _gen_savepoint_name(_c=count()) -> str:
    return f"tortoise_savepoint_{next(_c)}"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'
