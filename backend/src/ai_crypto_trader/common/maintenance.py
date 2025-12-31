import logging
from typing import List, Dict

from sqlalchemy import text

from ai_crypto_trader.common.database import engine

logger = logging.getLogger(__name__)


async def ensure_and_sync_paper_id_sequences() -> List[Dict[str, object]]:
    """
    For PostgreSQL, ensure paper_* tables have an id sequence/default and align it to max(id)+1.
    Creates the sequence if missing, sets default, and syncs setval.
    """
    async with engine.begin() as conn:
        if conn.dialect.name != "postgresql":
            logger.info("Skipping sequence maintenance; dialect is not PostgreSQL", extra={"dialect": conn.dialect.name})
            return []

        result = await conn.execute(
            text(
                "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = current_schema() AND tablename LIKE 'paper_%'"
            )
        )
        tables = [(row.schemaname, row.tablename) for row in result.fetchall()]

        adjustments: List[Dict[str, object]] = []
        for schema, table in tables:
            qualified_table = f"{schema}.{table}"
            seq_result = await conn.execute(text("SELECT pg_get_serial_sequence(:table, 'id')"), {"table": qualified_table})
            seq_name = seq_result.scalar()
            created_flag = False

            if seq_name and "." not in seq_name:
                seq_name = f"{schema}.{seq_name}"

            if not seq_name:
                seq_basename = f"{table}_id_seq"
                seq_name = f"{schema}.{seq_basename}"
                await conn.execute(text(f'CREATE SEQUENCE IF NOT EXISTS "{schema}"."{seq_basename}"'))
                await conn.execute(
                    text(
                        f'ALTER TABLE "{schema}"."{table}" ALTER COLUMN "id" SET DEFAULT nextval(\'"{schema}"."{seq_basename}"\'::regclass)'
                    )
                )
                await conn.execute(text(f'ALTER SEQUENCE "{schema}"."{seq_basename}" OWNED BY "{schema}"."{table}"."id"'))
                created_flag = True

            max_id_result = await conn.execute(text(f'SELECT COALESCE(MAX(id), 0) FROM "{schema}"."{table}"'))
            max_id = max_id_result.scalar() or 0
            next_val = max_id + 1

            await conn.execute(
                text("SELECT setval(CAST(:seq AS regclass), :next_val, false)"), {"seq": seq_name, "next_val": next_val}
            )
            adjustments.append(
                {"table": f"{schema}.{table}", "sequence": seq_name, "next_val": next_val, "created_at": created_flag}
            )
            logger.info(
                "Ensured and synced sequence for table",
                extra={"table": f"{schema}.{table}", "sequence": seq_name, "next_val": next_val, "created_at": created_flag},
            )

        return adjustments


async def fix_all_id_sequences() -> List[Dict[str, object]]:
    """
    For PostgreSQL, ensure every public base table with integer/bigint NOT NULL id has a sequence/default and is synced.
    Executes a DO block to create missing sequences, attach defaults, and setval to max(id)+1.
    """
    async with engine.begin() as conn:
        if conn.dialect.name != "postgresql":
            logger.info("Skipping id sequence fix; dialect is not PostgreSQL", extra={"dialect": conn.dialect.name})
            return []

        tables_result = await conn.execute(
            text(
                """
                SELECT table_schema, table_name
                FROM information_schema.columns c
                JOIN information_schema.tables t
                  ON c.table_schema = t.table_schema AND c.table_name = t.table_name
                WHERE t.table_type = 'BASE TABLE'
                  AND c.column_name = 'id'
                  AND c.table_schema = current_schema()
                  AND c.is_nullable = 'NO'
                  AND c.data_type IN ('integer', 'bigint')
                  AND c.identity_generation IS NULL
                  AND c.column_default IS NULL
                """
            )
        )
        targets = [(row.table_schema, row.table_name) for row in tables_result]

        if not targets:
            return []

        await conn.execute(
            text(
                """
                DO $do$
                DECLARE
                  r record;
                  seq_name text;
                BEGIN
                  FOR r IN (
                    SELECT c.table_schema, c.table_name
                    FROM information_schema.columns c
                    JOIN information_schema.tables t
                      ON c.table_schema = t.table_schema AND c.table_name = t.table_name
                    WHERE t.table_type = 'BASE TABLE'
                      AND c.column_name = 'id'
                      AND c.table_schema = current_schema()
                      AND c.is_nullable = 'NO'
                      AND c.data_type IN ('integer', 'bigint')
                      AND c.identity_generation IS NULL
                      AND c.column_default IS NULL
                  ) LOOP
                    seq_name := format('%I_id_seq', r.table_name);
                    EXECUTE format('CREATE SEQUENCE IF NOT EXISTS %I.%I', r.table_schema, seq_name);
                    EXECUTE format(
                      'ALTER TABLE %I.%I ALTER COLUMN id SET DEFAULT nextval(''%I.%I''::regclass)',
                      r.table_schema, r.table_name, r.table_schema, seq_name
                    );
                    EXECUTE format('ALTER SEQUENCE %I.%I OWNED BY %I.%I.id', r.table_schema, seq_name, r.table_schema, r.table_name);
                    EXECUTE format(
                      'SELECT setval(''%I.%I''::regclass, COALESCE(max(id),0)+1, false) FROM %I.%I',
                      r.table_schema, seq_name, r.table_schema, r.table_name
                    );
                  END LOOP;
                END
                $do$;
                """
            )
        )

        adjustments = []
        for schema, table in targets:
            seq_name = f"{table}_id_seq"
            adjustments.append({"table": f"{schema}.{table}", "sequence": f"{schema}.{seq_name}"})
            logger.info(
                "Ensured id sequence",
                extra={"table": f"{schema}.{table}", "sequence": f"{schema}.{seq_name}"},
            )

        return adjustments
