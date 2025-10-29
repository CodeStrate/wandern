from datetime import datetime
from wandern.databases.base import BaseProvider
from wandern.exceptions import ConnectError
from wandern.models import Config, Revision

import mysql.connector as mysql
from urllib.parse import urlparse, parse_qs
from typing import TypedDict, NotRequired, Literal


class MySQLConnectionParams(TypedDict):
    """TypedDict for MySQL connection parameters."""
    host: str
    port: int
    user: NotRequired[str]
    password: NotRequired[str]
    database: NotRequired[str]
    autocommit: NotRequired[bool]
    ssl_disabled: NotRequired[bool]
    use_pure: NotRequired[bool]


# structure for query string params
BOOLEAN_PARAM_KEYS: set[Literal['autocommit', 'ssl_disabled', 'use_pure']] = {
    'autocommit', 'ssl_disabled', 'use_pure'
}


def parse_params_from_dsn(dsn: str) -> MySQLConnectionParams:
    """
    Parse connection params for mysql-connector from provided dsn string.

    Args:
        dsn: str = The DSN (Data Source Name) syntax string.

    Returns:
        MySQLConnectionParams: A typed dictionary of parsed params. (eg. host, port, user, password)
    """
    if not dsn.startswith('mysql://'):
        raise ValueError("DSN string must start with mysql://")
    
    try:
        parsed_dsn = urlparse(dsn)
    except ValueError as e:
        raise ValueError(f"Failed to parse DSN: {e}") from e
    
    # Host and port are required
    if not parsed_dsn.hostname:
        raise ValueError("Host is required in DSN")
    if not parsed_dsn.port:
        raise ValueError("Port is required in DSN")
    
    # Build typed params dict with required fields
    parsed_params: MySQLConnectionParams = {
        'host': parsed_dsn.hostname,
        'port': parsed_dsn.port,
    }

    # Only add these if not None or empty
    if parsed_dsn.username:
        parsed_params['user'] = parsed_dsn.username
    if parsed_dsn.password:
        parsed_params['password'] = parsed_dsn.password  
    if parsed_dsn.path and parsed_dsn.path.strip('/'):
        parsed_params['database'] = parsed_dsn.path.lstrip('/')

    # Parse query parameters more tightly
    if parsed_dsn.query:
        try:
            query_params = parse_qs(parsed_dsn.query, strict_parsing=True, keep_blank_values=True)
            for key, value_list in query_params.items():
                # Ensure we have a non-empty list and take the first value
                if not value_list or not value_list[0]:
                    raise ValueError(f"Empty value for query parameter: {key}")
                value = value_list[0]
                
                # Only allow known parameters to be added to typed dict
                if key in BOOLEAN_PARAM_KEYS:
                    # These will be converted to bool in validation
                    parsed_params[key] = value  # type: ignore
                elif key in ('user', 'password', 'database'):
                    # Allow overriding from query params
                    parsed_params[key] = value  # type: ignore
                else:
                    # Ignore unknown parameters rather than adding them loosely
                    pass
        except ValueError as e:
            raise ValueError(f"Failed to parse query parameters: {e}") from e
    
    return parsed_params
    
def validate_parsed_params(params_dict: MySQLConnectionParams) -> MySQLConnectionParams:
    """
    Validate if the parsed params are syntactically correct and normalize the parameters.

    Args:
        params_dict: A typed dictionary of connection parameters.
    
    Returns:
        Validated and normalized parameters.
    """

    validated_params: MySQLConnectionParams = params_dict.copy()  # type: ignore

    # Validate and convert port to integer
    try:
        validated_params['port'] = int(validated_params['port'])
    except (ValueError, TypeError) as e:
        raise ValueError(f"Invalid port value: {validated_params.get('port')}") from e
    
    # Validate port range
    if not (1 <= validated_params['port'] <= 65535):
        raise ValueError(f"Port must be between 1 and 65535, got: {validated_params['port']}")
        
    # Convert boolean parameters from string to bool using the defined set
    for param in BOOLEAN_PARAM_KEYS:
        if param in validated_params:
            param_value = validated_params[param]  # type: ignore
            if isinstance(param_value, str):
                validated_params[param] = param_value.lower() in ('true', '1', 'yes', 'on')  # type: ignore
            elif not isinstance(param_value, bool):
                raise ValueError(f"Invalid value for boolean parameter '{param}': {param_value}")

    return validated_params

class MySQLProvider(BaseProvider):
    def __init__(self, config: Config):
        self.config = config

    def connect(self) -> mysql.MySQLConnection:
        """
        Establish connection to MySQL database.
        
        Returns a connection with autocommit enabled to ensure changes are persisted immediately.
        """
        try:
            params = parse_params_from_dsn(self.config.dsn)
            connection_params = validate_parsed_params(params)

            if 'autocommit' not in connection_params:
                connection_params['autocommit'] = True
                
            return mysql.connect(**connection_params)
        
        except Exception as exc:
            raise ConnectError(
                "Failed to connect to the database"
                f"\nIs your database server running on '{self.config.dsn}'?"
            ) from exc
    
    def create_table_migration(self) -> None:
        """
        Create the migrations tracking table.
        """

        # We use TIMESTAMP(6) as MySQL TIMESTAMP goes upto seconds as compared to SQLITE and Postgres
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.config.migration_table} (
            revision_id VARCHAR(255) PRIMARY KEY NOT NULL,
            down_revision_id VARCHAR(255),
            message TEXT,
            tags TEXT,
            author VARCHAR(255),
            created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6) 
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
            if revision.up_sql:
                cursor = connection.cursor()
                cursor.execute(revision.up_sql)
                if getattr(cursor, "with_rows", False):
                    cursor.fetchall()
                cursor.close()

            cursor = connection.cursor()
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
            rowcount = cursor.rowcount
            cursor.close()

            return rowcount

    def migrate_down(self, revision: Revision) -> int:
        query = f"""
        DELETE FROM {self.config.migration_table}
        WHERE revision_id = %(revision_id)s
        """

        with self.connect() as connection:
            if revision.down_sql:
                cursor = connection.cursor()
                cursor.execute(revision.down_sql)
                if getattr(cursor, "with_rows", False):
                    cursor.fetchall()
                cursor.close()

            cursor = connection.cursor()
            cursor.execute(query, {"revision_id": revision.revision_id})
            rowcount = cursor.rowcount
            cursor.close()

            return rowcount

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