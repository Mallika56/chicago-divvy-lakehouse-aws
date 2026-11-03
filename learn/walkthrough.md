# Weekend walkthrough: Chicago Divvy Lakehouse

This guide assumes you're starting from zero. Read it top to bottom once, then run the project
yourself, then try the exercises. Plan on **4–6 hours** across the weekend.

| Session | What to do | Time |
|---|---|---|
| 1 | Parts 1–3: the big picture and the vocabulary | 1 hr |
| 2 | Part 4: run it locally, then read `transform.py` with this guide open | 1.5–2 hrs |
| 3 | Part 5: AWS pieces (`template.yaml`, Glue job, Athena SQL) | 1–1.5 hrs |
| 4 | Parts 6–7: exercises + say the interview answers out loud | 1–1.5 hrs |

---

## Part 1: The project in one paragraph

Divvy publishes every bike ride in Chicago as monthly CSV files. This project builds the system
a company would use to turn those files into a clean, cheap-to-query table. Files are copied
untouched into a **raw** storage area. A Spark job cleans them, checks every row against data-quality
rules, and writes the good rows into a **curated** area in a fast columnar format (Parquet). Bad rows
go to a **quarantine** area along with the reason they failed. Analysts then query the curated data with
plain SQL in Athena. The infrastructure is defined as code, so it can be created or deleted with one
command.

**Your 30-second interview pitch:**
> "I built an AWS lakehouse for Chicago's Divvy bike-share data. Monthly files land in an S3 raw zone,
> and a Glue PySpark job standardizes them and applies 11 data-quality rules. Bad rows go to
> quarantine with a reason code instead of being dropped. Clean rows are written as Parquet,
> partitioned by year and month, and queried in Athena. Everything is in CloudFormation, and the
> transformation code is shared between Glue and local runs so it's unit-tested. Then I used it to
> answer a business question: when and where to target casual riders for memberships."

---

## Part 2: Vocabulary (learn these first)

| Term | Plain meaning | In this project |
|---|---|---|
| **Data lake** | Cheap file storage that holds data in any format | S3 buckets |
| **Lakehouse** | A data lake plus warehouse-like features: tables, schemas, SQL | S3 + Glue Catalog + Athena |
| **Zone / layer** | Folders for data at different levels of cleanliness | raw → curated (sometimes called bronze → silver) |
| **ETL** | Extract, Transform, Load | Glue job: read CSV → clean → write Parquet |
| **Parquet** | A columnar file format. It stores each column together, compresses well, and lets queries read only the columns they need | Curated zone |
| **Partition** | Splitting a table into folders by a column, e.g. `year=2026/month=7/` | Lets Athena skip months you didn't ask for |
| **Partition pruning** | The query engine skipping partitions that can't match your WHERE clause | `WHERE year=2026 AND month=7` reads 1 folder, not 12 |
| **Glue Data Catalog** | A central list of tables: names, columns, types and where the files are | `divvy_lake.divvy_trips` |
| **Glue Crawler** | A robot that scans files and guesses a table schema | Used on the raw zone |
| **Glue Job** | Managed Spark: you give it a script, AWS runs it on a cluster | `raw_to_curated.py` |
| **Athena** | Serverless SQL over files in S3. You pay per data scanned ($5/TB) | `sql/athena/*.sql` |
| **IaC (CloudFormation)** | Writing infrastructure as a file instead of clicking in the console | `infra/template.yaml` |
| **Idempotent** | Running it twice gives the same result as running it once | Overwrite mode + dedup |
| **Quarantine** | Where bad rows go, with a reason, so nothing disappears silently | `lake/quarantine/` |

---

## Part 3: The data

One row is one ride. There are 13 columns:

```
ride_id, rideable_type, started_at, ended_at,
start_station_name, start_station_id, end_station_name, end_station_id,
start_lat, start_lng, end_lat, end_lng, member_casual
```

Real-world quirks the pipeline handles. Each one is a good interview story:

1. **Duplicates across files.** A file holds rides that *ended* that month. A ride ending exactly
   on the boundary can appear in two files, so we keep one copy per `ride_id`.
2. **Negative durations.** When clocks "fall back" for daylight saving, a ride can appear to end
   before it started.
3. **Test and warehouse stations** like "Hastings Test Station" and "STAFF & VISITORS @ West Chicago
   Warehouse" are staff moving bikes, not customers.
4. **Station names with a trailing `*`** ("Lincoln Ave & Roscoe St*") mark temporary locations. We
   strip the `*` so one station has one name.
5. **Dockless e-bikes.** About 20% of rides have no start station, and their coordinates are rounded
   to 2 decimals.

**About the sample:** the repo holds 1 in every 256 rides (ride ids ending in `00`). The ids are
random hex, so it behaves like a random sample. Percentages are trustworthy. Raw counts are 1/256 of
the real totals.

---

## Part 4: Run it, then read the code

```bash
pip install -r requirements.txt
python scripts/run_local.py
python -m pytest -q
```

You'll see three steps print: ingest, transform (with the quarantine counts), and query results.
Now open `src/divvy_lakehouse/transform.py`. Here is the line-by-line tour.

### `RAW_SCHEMA`: read everything as strings
```python
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])
```
**Why:** if we let Spark *infer* types and a value is bad (e.g. `"N/A"` in a date column), Spark
silently turns it into NULL and we never find out. Reading as text and casting ourselves means a
bad value becomes a NULL we *detect* (rule `bad_timestamp`).

### `read_raw()`
```python
.withColumn("source_file", F.regexp_extract(F.input_file_name(), r"([^/]+\.csv)$", 1))
```
Records which file each row came from (**lineage**). It's needed for dedup and useful when debugging.

### `standardize()`
- `_blank_to_null`: turns `""` and `"   "` into real NULLs so `IS NULL` checks work.
- `F.lower(...)` on categories: `"Member"` and `"member"` become the same value.
- `regexp_replace(..., r"\s*\*+$", "")` strips the trailing `*` from station names.
- `to_timestamp(..., "yyyy-MM-dd HH:mm:ss[.SSS]")`: the `[...]` makes milliseconds optional.
- **No rows are dropped here.** Cleaning and filtering are separate steps on purpose.

### `add_derived()`: compute once, use everywhere
- `duration_min` = (end − start) in seconds / 60.
- `start_dow`: Spark's `dayofweek()` says Sunday = 1, but the ISO standard (and most business
  reports) says Monday = 1. `((dow + 5) % 7) + 1` converts it. Check: Sunday 1 → (6 % 7) + 1 = 7 ✓.
  Monday 2 → (7 % 7) + 1 = 1 ✓.
- `distance_km` uses the **haversine formula**: straight-line distance on a sphere. It isn't the
  route ridden, so say "straight-line distance" in interviews.
- `year`, `month` come from the ride **start** and become the partition columns.

**Why derive in the pipeline instead of in SQL?** Every analyst gets the same definition of
"weekend", and the SQL stays simple and portable. Athena and DuckDB number weekdays differently,
so pre-computing avoids that bug.

### `deduplicate()`: window functions
```python
w = Window.partitionBy("ride_id").orderBy(F.col("source_file").desc())
F.row_number().over(w)   # 1 for the newest file's copy, 2+ for older copies
```
This is the PySpark version of the SQL interview classic:
```sql
ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY source_file DESC)
```
Rows with `row_number > 1` are flagged `is_duplicate` and quarantined, not deleted.

### `apply_quality_rules()`: the heart of the project
The rules are a list of `(name, condition)` pairs. The loop builds one big `CASE WHEN`:
```
CASE WHEN missing_ride_id THEN 'missing_ride_id'
     WHEN duplicate       THEN 'duplicate_ride_id'
     ...
     ELSE NULL END
```
It loops over `reversed(rules)` so that the **first rule in the list wins**, which means the most
fundamental problem gets recorded. `F.coalesce(cond, False)` matters because in SQL,
`NULL > 5` is NULL (not False), and NULL inside CASE WHEN would fall through unpredictably.

### `split_valid_invalid()` and `transform()`
Valid rows (`dq_reason IS NULL`) go to curated; everything else goes to quarantine. The test
`test_curated_rows_obey_every_rule` asserts **curated + quarantine = rows read**, so nothing
is lost silently.

### `write_parquet()`: the small-files lesson
```python
df = df.repartition("year", "month")
```
Without this line, the first run wrote **2,368 tiny files totalling 33 MB, 7× bigger than the
raw CSV**. Spark writes one file per task per partition, and each tiny Parquet file carries its own
metadata overhead. With the line: **12 files, 1.9 MB**. Athena pays a cost per file it opens, so
small files make queries slower and pricier. This is a very common interview topic.

### The tests (`tests/`)
- `_row(**overrides)` builds one clean ride; each test changes one field to break one rule.
- `@pytest.mark.parametrize` runs the same test 10 times with different inputs.
- The edge-case fixture uses **real** awkward rows, not just made-up ones.
- A bug the tests caught while building this: `RAW_SCHEMA.add(...)` *modifies* the shared schema
  object, so it gained a duplicate `source_file` column. The fix is to build a new `StructType`.
  Lesson: watch for mutable shared objects.

---

## Part 5: The AWS pieces

### `infra/template.yaml` (CloudFormation)
Read it section by section. The points worth knowing:
- **Four buckets** (raw, curated, athena-results, artifacts). All are encrypted, and public access
  is blocked on all of them.
- **Raw bucket has versioning** (undo a bad overwrite) and a **lifecycle rule** moving files to
  Glacier Instant Retrieval after 90 days (cheaper storage for data you rarely read).
- **Athena results expire after 7 days**, so query output doesn't pile up forever.
- **IAM role with least privilege:** Glue can *read* raw and artifacts and *write* only to curated.
  Exam phrase: "grant only the permissions required."
- **Glue table with partition projection:** Athena calculates partition folder paths from
  `projection.year.range` etc., so no crawler or `MSCK REPAIR TABLE` is needed when a new month
  arrives.
- **Crawler `SchemaChangePolicy: LOG`:** if Divvy changes its columns, it logs instead of silently
  changing the table.
- **Athena workgroup `BytesScannedCutoffPerQuery: 1 GB`:** a runaway `SELECT *` gets cancelled
  instead of running up a bill.
- **EventBridge rule → SNS:** email when the Glue job fails.

### `glue_jobs/raw_to_curated.py`
A thin wrapper: get arguments, create the Spark session through `GlueContext`, call the *same*
`transform()` function, write output, save metrics JSON. `partitionOverwriteMode=dynamic` means
re-running only replaces the months being written, not the whole table.

### `sql/athena/`
- `01_create_tables.sql`: the same table as the CloudFormation one, written in SQL for learning.
- `02`–`07`: analytics. They use `COUNT(*) OVER ()` (a window over the whole result) for
  percentage-of-total, `CASE WHEN` inside `SUM` for conditional counts, a CTE in `05`, and
  `approx_percentile` (Athena's fast median).

---

## Part 6: Try it yourself

1. **Add a rule.** Quarantine rides where `distance_km > 50`. Add it to `rules`, add a
   parametrized test case, and run `pytest`.
2. **Prove partition pruning.** In DuckDB, run `EXPLAIN` on a query with and without
   `WHERE year = 2026 AND month = 7` and compare how many files are read.
3. **New analysis.** Write `sql/athena/08_ebike_by_hour.sql`: the e-bike share of rides by hour of
   day. Run `python scripts/run_queries.py`.
4. **Break the pipeline on purpose.** Edit one sample CSV and change a header to `bike_type`. Run
   the ingest. Which function stops it, and why is failing loudly better?
5. **Remove `repartition()`** in `write_parquet`, re-run, and count the files in
   `lake/curated/` with `find lake/curated -name '*.parquet' | wc -l`. Put it back afterwards.

---

## Part 7: Interview questions (practice saying these out loud)

**1. Walk me through your pipeline.**
Use the 30-second pitch, then go deeper on whichever part they ask about.

**2. Why Parquet instead of CSV?**
Parquet is columnar. A query reading 3 of 25 columns only reads those 3, and similar values stored
together compress well. In this project the curated Parquet is 60% smaller than the raw CSV even
though it has 12 more columns. Athena charges per byte scanned, so smaller and column-pruned files
directly lower the cost.

**3. How did you choose partition columns?**
I partitioned by the year and month the ride started, because nearly every analysis filters on time
and monthly loads line up with how Divvy publishes. I avoided high-cardinality columns like
station, because that would create thousands of tiny partitions.

**4. How do you handle bad data?**
I check 11 rules in a fixed order and tag each failing row with the first rule it breaks. Those rows
go to a quarantine table instead of being deleted. A test asserts curated + quarantine = rows read,
so nothing disappears. On the sample, 2.6% of rows were quarantined, mostly rides under one minute.

**5. How do you handle duplicates?**
A window function: `ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY source_file DESC)` keeps the
copy from the latest file and flags the others as duplicates.

**6. What's the small-files problem?**
Many tiny files make query engines slow, because each file adds open and metadata overhead and
there are too many tasks. My first run produced 2,368 small files, 7× the raw size. I
repartitioned by the partition columns before writing and got 12 files and 1.9 MB.

**7. Crawler vs. defining the table yourself? What is partition projection?**
The crawler is good for discovering a schema you don't control, so I used it on raw. For the
curated table I own the schema, so I defined it in CloudFormation where it gets code review.
Partition projection lets Athena compute partition locations from rules, so new months are
queryable immediately without running a crawler or `MSCK REPAIR TABLE`.

**8. How did you control cost and security?**
Cost: Parquet and partitions, an Athena 1 GB scan cap per query, a lifecycle rule moving raw to
Glacier Instant Retrieval, query results expiring after 7 days, and a 2-worker Glue job with a
30-minute timeout. Security: encryption on every bucket, public access blocked, and a
least-privilege IAM role (Glue can only write to curated). Next I'd add Lake Formation
column-level permissions.

**Bonus, the business side:** "What did you find?" Members commute: 8.4% of their weekday rides
start in the 8am hour, versus 4.4% for casual riders. Casual riders ride longer and on weekends,
and they cluster at the lakefront and museums. The casual share swings from 42% in June to 17% in
January. So I'd recommend a summer membership campaign at those stations that focuses on leisure
value.

---

## Resume bullets (pick 2)

- Built an AWS lakehouse (S3, Glue PySpark, Athena, CloudFormation) for Chicago Divvy bike-share
  data, with 11 data-quality rules that quarantine invalid records with reason codes and 23 pytest
  tests covering real-data edge cases
- Cut curated storage from 7× to 0.4× raw size by fixing a Spark small-files issue (2,368 files
  → 12), and added partition projection and a per-query scan cap to keep Athena costs bounded
- Analyzed rider behavior with Athena SQL (window functions, CTEs); found casual-rider share swings
  from 42% to 17% seasonally and concentrates at lakefront stations, which shaped a targeted
  membership-campaign recommendation
