import dlt
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# SCD Type 1 — lookup_zones_scd1
# Overwrite changes (no history)
# ──────────────────────────────────────────────
dlt.create_streaming_table(
    name="gold.dim_location_scd1",
    comment="Lookup zones - SCD Type 1 (overwrite, no history)",
    table_properties={"delta.feature.timestampNtz": "supported"}
)

dlt.apply_changes(
    target="gold.dim_location_scd1",
    source="lookup_zones_raw",
    keys=["location_id"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=1,
    ignore_null_updates=True
)


# ──────────────────────────────────────────────
# SCD Type 2 — lookup_zones_scd2
# History maintained
# ──────────────────────────────────────────────
dlt.create_streaming_table(
    name="gold.dim_location_scd2",
    comment="Lookup zones - SCD Type 2 (history maintained)",
    table_properties={"delta.feature.timestampNtz": "supported"}
)

dlt.apply_changes(
    target="gold.dim_location_scd2",
    source="lookup_zones_raw",
    keys=["location_id"],
    sequence_by=F.col("ingestion_timestamp"),
    stored_as_scd_type=2,
    track_history_column_list=["borough", "zone", "service_zone"],
    ignore_null_updates=True
)