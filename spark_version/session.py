"""Single place where the SparkSession is created.

Every script and test gets its session from here, so all runs (development,
benchmark, live demo) use exactly the same Spark configuration. That is what
makes the pandas vs. Spark measurements comparable.
"""

from pyspark.sql import SparkSession

DEFAULT_APP_NAME = "proyecto-nota3"
DEFAULT_DRIVER_MEMORY = "4g"


def build_spark_session(
    app_name: str = DEFAULT_APP_NAME,
    driver_memory: str = DEFAULT_DRIVER_MEMORY,
    master: str = "local[*]",
) -> SparkSession:
    """Create (or reuse) the SparkSession used by the whole project.

    Args:
        app_name: Name shown in the Spark UI (http://localhost:4040).
        driver_memory: JVM heap size. In local mode the driver also runs the
            executors, so this is the total memory Spark may use. It is kept
            well below the machine RAM on purpose: Spark must finish the job
            with less memory than the dataset needs, which pandas cannot do.
        master: "local[*]" runs one executor thread per CPU core.

    Returns:
        A ready-to-use SparkSession.

    Example:
        >>> spark = build_spark_session()
        >>> spark.range(10).count()
        10
    """
    return (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Must be set before the JVM starts; it has no effect on a running session.
        .config("spark.driver.memory", driver_memory)
        # Fixed time zone: timestamps give the same result on every machine.
        .config("spark.sql.session.timeZone", "UTC")
        # Keep console output readable during benchmarks and the live demo.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
