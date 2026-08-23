import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as parquet


TABLES_QUERY = (
    "SELECT table_name FROM information_schema.tables "
    "WHERE table_schema = 'iox'"
)


def _setting(settings, name, default):
    value = settings.get(name, default) if settings else default
    return value if value not in (None, "") else default


def process_request(influxdb3_local, query_parameters, request_headers, request_body, args=None):
    """Handle ``POST /api/v3/engine/export_database``."""
    del query_parameters, request_headers

    settings = dict(args or {})
    if request_body:
        try:
            request_settings = json.loads(
                request_body.decode("utf-8") if isinstance(request_body, bytes) else request_body
            )
        except (TypeError, ValueError) as error:
            return {"status": 400, "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": f"Invalid export settings: {error}"})}
        if not isinstance(request_settings, dict):
            return {"status": 400, "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "Export settings must be a JSON object"})}
        settings.update(request_settings)

    export_dir = Path(_setting(settings, "export_dir", "/export/db"))
    compression = str(_setting(settings, "compression", "zstd")).lower()
    compression_level = int(_setting(settings, "compression_level", "6"))

    export_dir.mkdir(parents=True, exist_ok=True)
    table_rows = influxdb3_local.query(TABLES_QUERY)

    rows = table_rows.to_pylist() if hasattr(table_rows, "to_pylist") else table_rows
    table_names = [row["table_name"] for row in rows]

    exported = []
    failures = []

    for table_name in table_names:
        destination = export_dir / f"{table_name}.parquet"
        try:
            destination.unlink(missing_ok=True)
            table = influxdb3_local.query(
                f'SELECT * FROM "{str(table_name)}"'
            )

            if isinstance(table, list):
                table = pa.Table.from_pylist(table)

            parquet.write_table(
                table,
                destination,
                compression=compression,
                compression_level=compression_level,
            )
            exported.append(str(destination))
        except Exception as error:
            failures.append({"table": table_name, "error": str(error)})
            influxdb3_local.error(f"Failed to export table {table_name}: {error}")

    result = {
        "tables_found": len(table_names),
        "tables_exported": len(exported),
        "files": exported,
        "failures": failures,
    }
    return json.dumps(result)
    