"""SQLAlchemy database loader — writes a DataFrame to any SQL database."""

from __future__ import annotations

import logging
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text, Engine, inspect

from data_extractor.loaders.base import BaseLoader
from data_extractor.registry import register_loader

logger = logging.getLogger(__name__)


@register_loader("sql_database")
class SQLAlchemyLoader(BaseLoader):
    """Persist a DataFrame to a SQL database via SQLAlchemy."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._engine: Engine | None = None

    def connect(self) -> None:
        connection_string = self._config["connection_string"]
        self._engine = create_engine(connection_string)
        logger.info("Connected to database: %s", connection_string)

    def load(self, df: pd.DataFrame) -> None:
        if self._engine is None:
            self.connect()

        table_name = self._config["table_name"]
        if_exists = self._config.get("if_exists", "append")
        index = self._config.get("index", False)
        chunksize = self._config.get("chunksize", None)
        method = self._config.get("method", None)

        if if_exists == "upsert":
            primary_keys = self._config.get("primary_keys")
            if not primary_keys:
                raise ValueError(
                    "primary_keys must be specified when if_exists='upsert'"
                )
            self._upsert(df, table_name, primary_keys, index)
            return

        df.to_sql(
            name=table_name,
            con=self._engine,
            if_exists=if_exists,
            index=index,
            chunksize=chunksize,
            method=method,
        )
        logger.info(
            "Wrote %d rows to table %r (if_exists=%s)",
            len(df),
            table_name,
            if_exists,
        )

    def disconnect(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
            logger.info("Disposed SQLAlchemy engine")

    # ------------------------------------------------------------------
    # Upsert support
    # ------------------------------------------------------------------

    def _upsert(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: list[str],
        index: bool,
    ) -> None:
        """INSERT … ON CONFLICT DO UPDATE for each row."""
        if df.empty:
            logger.info("Empty DataFrame — skipping upsert for %r", table_name)
            return

        assert self._engine is not None
        self._ensure_table(df, table_name, primary_keys, index)

        dialect = self._engine.dialect.name
        if dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert as dialect_insert
        elif dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as dialect_insert
        else:
            raise NotImplementedError(
                f"Upsert not implemented for dialect {dialect!r}"
            )

        metadata = self._reflect_table(table_name)
        table = metadata.tables[table_name]

        non_pk_cols = [c for c in df.columns if c not in primary_keys]

        records = df.to_dict(orient="records")
        with self._engine.begin() as conn:
            for record in records:
                stmt = dialect_insert(table).values(**record)
                if non_pk_cols:
                    update_dict = {c: stmt.excluded[c] for c in non_pk_cols}
                    stmt = stmt.on_conflict_do_update(
                        index_elements=primary_keys,
                        set_=update_dict,
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(
                        index_elements=primary_keys,
                    )
                conn.execute(stmt)

        logger.info(
            "Upserted %d rows into table %r (primary_keys=%s)",
            len(df),
            table_name,
            primary_keys,
        )

    def _ensure_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        primary_keys: list[str],
        index: bool,
    ) -> None:
        """Create the table (if missing) with a unique index on *primary_keys*."""
        assert self._engine is not None

        if inspect(self._engine).has_table(table_name):
            return

        # Create schema from an empty DataFrame
        df.head(0).to_sql(
            name=table_name,
            con=self._engine,
            if_exists="fail",
            index=index,
        )

        # Add unique index on primary key columns
        pk_cols = ", ".join(primary_keys)
        idx_name = f"uq_{table_name}_{'_'.join(primary_keys)}"
        with self._engine.begin() as conn:
            conn.execute(
                text(
                    f"CREATE UNIQUE INDEX {idx_name} "
                    f"ON {table_name} ({pk_cols})"
                )
            )
        logger.info(
            "Created table %r with unique index on %s", table_name, primary_keys
        )

    def _reflect_table(self, table_name: str):
        """Return a MetaData object with the named table reflected."""
        from sqlalchemy import MetaData

        assert self._engine is not None
        metadata = MetaData()
        metadata.reflect(bind=self._engine, only=[table_name])
        return metadata
