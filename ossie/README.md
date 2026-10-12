# Ossie hub doc — vendored, not authored here

The Ossie semantic-model document is authored and regenerated in the Snowflake
repo (`hyrox-tieredsla-pipeline/ossie/`), from the dbt semantic models via
`ossie-dbt msi-to-ossie`. This repo consumes it; it does not define it.

When the semantic layer lands (phase 5):

1. Copy `ossie/semantic_model.yaml` from the Snowflake repo into this folder.
2. Export the Databricks semantic layer definition from the hub doc using the
   Ossie Databricks converter (`converters/databricks` in apache/ossie).
3. The Snowflake side exports the Snowflake semantic view (Cortex Analyst)
   from the same hub doc. One model, two consumers — that is the whole point.

Do not hand-edit the vendored file. Regenerate at the source.
