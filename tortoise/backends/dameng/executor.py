from tortoise import Model
from tortoise.backends.odbc.executor import ODBCExecutor
from tortoise.exceptions import OperationalError
from tortoise.fields import BigIntField, IntField, SmallIntField


class DamengExecutor(ODBCExecutor):
    async def execute_insert(self, instance: Model) -> None:
        if instance._custom_generated_pk and instance._meta.pk.generated:
            table = instance._meta.db_table
            await self.db.execute_script(f"SET IDENTITY_INSERT {table} ON")
            try:
                await super().execute_insert(instance)
            finally:
                await self.db.execute_script(f"SET IDENTITY_INSERT {table} OFF")
            return

        await super().execute_insert(instance)

    async def _process_insert_result(self, instance: Model, results: int) -> None:
        pk_field_object = self.model._meta.pk
        if (
            isinstance(pk_field_object, (SmallIntField, IntField, BigIntField))
            and pk_field_object.generated
            and not instance._custom_generated_pk
        ):
            try:
                row = (await self.db.execute_query_dict("SELECT global_identity"))[0]
            except (IndexError, OperationalError):
                pass
            else:
                instance.pk = next(iter(row.values()))
        if self.model._meta.db_default_db_columns:
            await self._fetch_db_defaults_after_insert(instance)
