from tortoise import Model
from tortoise.backends.odbc.executor import ODBCExecutor
from tortoise.exceptions import OperationalError
from tortoise.fields import BigIntField, IntField, SmallIntField


class DamengExecutor(ODBCExecutor):
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
