# DuckDB vs Unity Catalog Serverless SQL 

Harness, synthetic generator, shared queries, and published result CSVs for
*DuckDB versus Unity Catalog Serverless SQL for Object-Store Analytics*.


`results/benchmark_results.csv` is the trace behind the paper. It is 696
rows: 58 query ids, two engines, three cold attempts and three warm
attempts each. There is no ds10.

| Column | Published values |
|--------|------------------|
| Engine | `duckdb`, `databricks_uc_serverless` |
| Warehouse size | `Small` on UC rows |
| Tables | ds1–ds7 and ds9, each as Parquet and as UC-managed Iceberg |

Headline numbers in the paper are **cold attempt 1**. Warm numbers are the
median of the last two of three warm attempts. They are recorded and
plotted, and they are not the comparison.

Cold attempt 1 in this CSV is a first touch on a warehouse that was already
up for the suite, and a new DuckDB connection. It is not a warehouse
restart before each query, and it does not drop the OS page cache. UC cold
attempts 2 and 3 are result-cache hits. A per-query restart would add tens
of seconds of UC spin-up on top of the 1–4 s first-statement band.

The harness default in `config/config.example.yaml` is different from this
CSV: `cold_repetitions: 1` and `restart_warehouse_for_cold_run: true`.
Re-running with that default will not reproduce these rows.

## Systems

DuckDB is in-process with `httpfs`. Parquet is
`read_parquet('s3://…')`. Iceberg attaches the Unity Catalog Iceberg REST
catalog and reads the managed table by name. The paper pins 4 threads, a
12 GB memory limit, and spill at `/var/duckdb-tmp` on a dedicated
Guaranteed pod (4 vCPU, 12 GiB, 20 Gi ephemeral) in `us-east-2`. The CSV
warehouse-size column for those rows is `duckdb-local`.

UC Serverless is a Small SQL warehouse (4 DBU/h). Parquet is a UC external
table. Iceberg is a managed `CREATE TABLE … USING ICEBERG AS SELECT`.
Wall time includes execute plus `fetchall()`. Bytes scanned were not
available from query history in this campaign.

The comparison is those engines plus those access paths. It is not matched
I/O.

Every table is synthetic. Columns are `col_0`..`col_N`, types cycling
`BIGINT` / `DOUBLE` / `VARCHAR` / `BOOLEAN` / `TIMESTAMP`. There is no
customer data, no workspace tokens, and no source-system schema in this
tree.

## Layout

```
benchmark/           harness (generate, setup, run, run-duckdb)
benchmark/queries.yaml
config/config.example.yaml
k8s/duckdb-bench-job.yaml
results/benchmark_results.csv
tests/
```

## Prerequisites

- Python 3.11+
- AWS CLI configured for the S3 bucket you will use (paper-scale run)
- A Databricks workspace with a Serverless SQL warehouse and Unity Catalog
  rights to create a catalog/schema, a storage credential, and an external
  location
- DuckDB (pulled in via `requirements.txt`)
- Paper DuckDB host: dedicated **Guaranteed** pod on **m6i.xlarge** (4 vCPU,
  16 GiB, `us-east-2`), `cpu: "4"`, `memory: 12Gi`, `emptyDir` 20Gi at
  `/var/duckdb-tmp`. Do not run the paper numbers on a BestEffort shared
  apps node or a laptop.

Secrets stay out of git:

```bash
cp config/config.example.yaml config/config.yaml   # edit host, http_path, bucket, IAM role
export DATABRICKS_TOKEN=...                        # or DATABRICKS_CLIENT_SECRET for OAuth M2M
```

## Reproduce

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt

# 1. Land synthetic Parquet on S3 (paper-scale shapes in config.example.yaml)
python -m benchmark.cli generate --config config/config.yaml

# 2. Register external Parquet tables
python -m benchmark.cli setup --config config/config.yaml

# 3. CTAS Iceberg twins (ds1_iceberg .. ds9_iceberg)
python -m benchmark.cli setup-managed-iceberg --config config/config.yaml

# 4. Write-time sorted Iceberg of ds9 -> ds10
python -m benchmark.scripts.convert_to_sorted_iceberg \
  --config config/config.yaml \
  --source-table ds9 \
  --dest-table ds10 \
  --sort-columns col_0,col_1,col_2,col_3,col_4,col_5,col_6,col_7,col_8,col_9

# 5. Paper numbers: both engines in k8s/duckdb-bench-job.yaml on a
#    tainted m6i.xlarge (Databricks warehouse is still remote; the Job
#    writes one CSV). cold = 1 after warehouse restart / new DuckDB
#    process; warm = 3, recorded but not the headline.
#    kubectl apply -f k8s/duckdb-bench-job.yaml
#    kubectl cp <pod>:/app/results/benchmark_results.csv ./results/
```

`results/benchmark_results.csv` is the published trace. Re-running appends
new rows; pass `--force` to re-execute query ids that already have a row
for that engine. Headline numbers are **cold** latency (one attempt).
Warm rows are recorded but not the comparison.

Paper protocol (also in `config.example.yaml`): DuckDB `threads: 4`,
`memory_limit: 12GB`, `temp_directory: /var/duckdb-tmp`,
`restart_warehouse_for_cold_run: true`. Access paths differ by design:
DuckDB Parquet is `read_parquet('s3://…')`; Databricks Parquet is a UC
external table; Iceberg is DuckDB Iceberg REST vs Databricks managed
CTAS. The comparison is engine plus those access paths, on Small
serverless SQL vs the m6i.xlarge pod above.

## Local smoke (no warehouse, no S3)

```bash
cp config/config.example.yaml config/config.yaml
# set s3.local_data_dir: data  and dummy host/bucket values
python -m benchmark.cli generate --config config/config.yaml --no-upload --smoke
python -m benchmark.cli run-duckdb --config config/config.yaml
```

Iceberg queries are skipped in local-dir mode. `--smoke` caps each table at
2,000 rows so the generator finishes in seconds; query *shapes* (column
counts, CAST + ORDER BY + LIMIT) stay the same.

## Table shapes (paper scale)

| ID   | Rows     | Cols | Role                          |
|------|----------|------|-------------------------------|
| ds1  | 4,424,286 | 20 | large narrow scan            |
| ds2  | 679      | 20 | tiny table                    |
| ds3  | 777,156  | 17 | mid narrow scan               |
| ds4  | 1,404,840 | 16 | mid-large narrow scan        |
| ds5  | 30,726   | 12 | small table                   |
| ds6  | 7,906    | 11 | cast vs no-cast               |
| ds7  | 11,512   | 11 | unbounded LIMIT               |
| ds9  | 500,000  | 1000 | wide synthetic              |
| ds10 | 500,000  | 1000 | sorted Iceberg of ds9 (not in this CSV) |

## Tests

```bash
pip install -r requirements-dev.txt
pytest -q
```

## License

MIT. See `LICENSE`.
