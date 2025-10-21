from datetime import datetime
from wandern.databases.base import BaseProvider
from wandern.exceptions import ConnectError
from wandern.models import Config, Revision

import mysql.connector as mysql
from urllib.parse import urlparse, parse_qs
from typing import Dict, Any

def parse_params_from_dsn(dsn: str) -> Dict[str, Any]: # str key, Any value
    """
    Parse connection params for mysql-connector from provided dsn string.

    Args:
        dsn: str = The DSN (Data Source Name) syntax string.

    Returns:
        Dict[str, Any]: A dictionary of parsed params. (eg. host, post, user, password)
    """
    if not dsn.startswith('mysql://'):
        raise ValueError("DSN string must start with mysql://")
    
    try:
        parsed_dsn = urlparse(dsn)
        # dict to store the params, host and port added as required and default.

        parsed_params = {
            'host' : parsed_dsn.hostname or '127.0.0.1',
            'port' : parsed_dsn.port or 3306,
        }

        # Only add these if not None or empty
        if parsed_dsn.username:
            parsed_params['user'] = parsed_dsn.username
        if parsed_dsn.password:
            parsed_params['password'] = parsed_dsn.password  
        if parsed_dsn.path and parsed_dsn.path.strip('/'):
            parsed_params['database'] = parsed_dsn.path.lstrip('/')

        # Optionally parse any provided query parameters
        if parsed_dsn.query:
            query_params = parse_qs(parsed_dsn.query)
            for k, v in query_params.items():
                # the values are lists, we are taking the first value
                if isinstance(v, list) and len(v) > 0:
                    parsed_params[k] = v[0]
                else:
                    parsed_params[k] = v
        return parsed_params
    
    except SyntaxError:
        raise
    except Exception as e:
        raise Exception(f'Encountered an issue: {e}')
    
def validate_parsed_params(params_dict: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate if the parsed params are syntactically correct and normalize the parameters.

    Args:
        params_dict: A dictionary of connection parameters.
    
    Returns:
        Validated and normalized parameters.
    """

    validated_params = params_dict.copy() # non destructive

    if validated_params['port'] != 3306:
        try:
            validated_params['port'] = int(validated_params['port'])
        except (ValueError, TypeError):
            raise ValueError(f"Invalid port value : {validated_params['port']}")
        
    # take common boolean parameters and convert them from str to bool; we can add in more in future.
    bool_params = ['autocommit', 'ssl_disabled', 'use_pure']
    for p in bool_params:
        if p in validated_params:
            validated_params[p] = validated_params[p] in ('true', '1', 'yes', 'on')

    return validated_params

class MySQLProvider(BaseProvider):
    def __init__(self, config: Config):
        self.config = config
        self.connection_params = {}

    def connect(self) -> mysql.MySQLConnection:

        try:
            params = parse_params_from_dsn(self.config.dsn)
            self.connection_params = validate_parsed_params(params)
            return mysql.connect(**self.connection_params)
        
        except Exception as exc:
            raise ConnectError(
                "Failed to connect to the database"
                f"\nIs your database server running on '{self.config.dsn}'?"
            ) from exc
    
    def create_table_migration(self) -> None:
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.migration_table} (
            revision_id TEXT PRIMARY KEY NOT NULL,
            down_revision_id TEXT,
            message TEXT,
            tags TEXT,
            author TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """

        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)

    def drop_table_migration(self) -> None:
        query = f"""
        DROP TABLE IF EXISTS {self.config.migration_table}
        """

        with self.connect() as connection:
            cursor = connection.cursor()
            cursor.execute(query)

    def get_head_revision(self) -> Revision | None:
        query = f"""
        SELECT * FROM {self.config.migration_table}
        ORDER BY created_at DESC LIMIT 1
        """

        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(query)
            row = cursor.fetchone()
            if not row:
                return None

            # Convert tags from TEXT to list
            tags = row["tags"].split(",") if row["tags"] else []

            return Revision(
                revision_id=row["revision_id"],
                down_revision_id=row["down_revision_id"],
                message=row["message"] or "",
                tags=tags,
                author=row["author"],
                created_at=(
                    row["created_at"] if row["created_at"] else datetime.now()
                ),
            )

    def migrate_up(self, revision: Revision) -> int:
        query = f"""
        INSERT INTO {self.config.migration_table}
            (revision_id, down_revision_id, message, tags, author, created_at)
        VALUES (%(revision_id)s, %(down_revision_id)s, %(message)s, %(tags)s, %(author)s, %(created_at)s)
        """

        with self.connect() as connection:
            cursor = connection.cursor()

            if revision.up_sql:
                cursor.execute(revision.up_sql)

            cursor.execute(
                query,
                {
                    "revision_id": revision.revision_id,
                    "down_revision_id": revision.down_revision_id,
                    "message": revision.message,
                    "tags": ",".join(revision.tags) if revision.tags else None,
                    "author": revision.author,
                    "created_at": datetime.now(),
                },
            )

            return cursor.rowcount

    def migrate_down(self, revision: Revision) -> int:
        query = f"""
        DELETE FROM {self.config.migration_table}
        WHERE revision_id = %(revision_id)s
        """

        with self.connect() as connection:
            cursor = connection.cursor()

            if revision.down_sql:
                cursor.execute(revision.down_sql)

            cursor.execute(query, {"revision_id": revision.revision_id})

            return cursor.rowcount

    def list_migrations(
        self,
        author: str | None = None,
        tags: list[str] | None = None,
        created_at: datetime | None = None,
    ) -> list[Revision]:
        base_query = f"""
        SELECT * FROM {self.config.migration_table}
        """

        where_clause = []
        params = {}

        if author:
            where_clause.append("author = %(author)s")
            params["author"] = author
        if tags:
            # For MySQL, we stored tags as comma-separated string
            # Check if any of the requested tags are in the stored tags
            tag_conditions = []
            for i, tag in enumerate(tags):
                tag_param = f"tag_{i}"
                tag_conditions.append(
                    f"(tags IS NOT NULL AND (tags = %({tag_param})s OR tags LIKE %({tag_param}_prefix)s OR tags LIKE %(suffix_{tag_param})s OR tags LIKE %(middle_{tag_param})s))"
                )
                params[tag_param] = tag
                params[f"{tag_param}_prefix"] = f"{tag},%"
                params[f"suffix_{tag_param}"] = f"%,{tag}"
                params[f"middle_{tag_param}"] = f"%,{tag},%"
            if tag_conditions:
                where_clause.append(f"({' OR '.join(tag_conditions)})")
        if created_at:
            where_clause.append("created_at >= %(created_at)s")
            params["created_at"] = created_at

        if where_clause:
            base_query += f" WHERE {' AND '.join(where_clause)}"
        base_query += " ORDER BY created_at DESC"

        with self.connect() as connection:
            cursor = connection.cursor(dictionary=True)
            cursor.execute(base_query, params)
            rows = cursor.fetchall()

            revisions = []
            for row in rows:
                # Convert tags from TEXT to list
                tags_list = row["tags"].split(",") if row["tags"] else []

                revisions.append(
                    Revision(
                        revision_id=row["revision_id"],
                        down_revision_id=row["down_revision_id"],
                        message=row["message"] or "",
                        tags=tags_list,
                        author=row["author"],
                        created_at=(
                            row["created_at"] if row["created_at"] else datetime.now()
                        ),
                    )
                )

            return revisions