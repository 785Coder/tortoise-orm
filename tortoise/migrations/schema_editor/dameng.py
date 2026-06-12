from tortoise.migrations.schema_editor.oracle import OracleSchemaEditor


class DamengSchemaEditor(OracleSchemaEditor):
    DIALECT = "dameng"
