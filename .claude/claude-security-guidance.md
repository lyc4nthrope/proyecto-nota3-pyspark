# Security guidance — proyecto-nota3-pyspark

Course project: batch data processing with pandas and PySpark. No network service, no authentication, no user accounts.

## Must never happen

- Committing datasets, benchmark outputs, `.env` files, credentials or tokens. `data/` and `output/` are git-ignored; keep it that way.
- Running external commands through a shell string. `benchmark/measure.py` launches processes with an argument **list** (`psutil.Popen(command)`); never use `shell=True`, `os.system` or string concatenation to build a command.
- Building the measured command from free text. Use `benchmark.measure.build_command`, which only accepts the engines in `ENGINES`.
- Leaving child processes alive after a timeout. Always stop the whole tree with `_stop_process_tree` (Spark's JVM is a child process).
- Downloading data from anywhere other than the sources listed in `docs/CONTRATO_ANALISIS.md`, or executing downloaded content.
- Using `pickle`, `eval` or `exec` on data files. Inputs are Parquet and CSV only.
- Writing outside the repository tree. Outputs go under `output/`, samples under `data/samples/`.

## Use instead

| Need | Use |
|---|---|
| Run and measure a version | `benchmark.measure.measure(build_command(engine, sample))` |
| Create a Spark session | `spark_version.session.build_spark_session()` |
| Read input | `pandas.read_parquet` / `spark.read.parquet` with explicit column lists |
