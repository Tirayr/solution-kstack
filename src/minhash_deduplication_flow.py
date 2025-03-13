import hashlib
import re
import struct
import os
from itertools import tee
from datetime import timedelta
from typing import Iterable, List, Tuple, Dict, Any

import numpy as np
from prefect import task, flow, get_run_logger
from prefect.variables import Variable
from prefect.tasks import task_input_hash
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType
from scipy.integrate import quad as integrate

# Constants
SEED = 42
NON_ALPHA = re.compile("[^A-Za-z_0-9]")
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)


def store_in_gcs(df, filepath):
    logger = get_run_logger()

    staging_gcs_bucket = Variable.get("staging_gcs_bucket")
    staging_dir = Variable.get("staging_dir")
    df.write.format("delta").mode("overwrite").save(
        os.path.join(staging_gcs_bucket, staging_dir, filepath)
    )
    logger.info(f"Stored {os.path.join(staging_gcs_bucket, staging_dir, filepath)}")


def read_from_gcs(spark, filepath):
    staging_gcs_bucket = Variable.get("staging_gcs_bucket")
    staging_dir = Variable.get("staging_dir")

    df = spark.read.format("delta").load(os.path.join(staging_gcs_bucket, staging_dir, filepath))
    return df


@task(name="Initialize Spark Session")
def init_spark_session(app_name: str = "MinHashLSH") -> SparkSession:
    """
    Initialize and return a Spark session.

    Parameters
    ----------
    app_name : str
        The name of the Spark application.

    Returns
    -------
    SparkSession
        The initialized Spark session.
    """
    logger = get_run_logger()
    logger.info(f"Initializing Spark session with app name: {app_name}")

    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")

    spark = (
        SparkSession.builder.config(conf=conf)
        .config("spark.executor.memory", "10g")
        .config("spark.driver.memory", "4g")
        .config(
            "spark.jars.packages",
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1,"
            + "io.delta:delta-spark_2.12:3.3.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    #
    spark.conf.set("credentialsFile", "resounding-keel-378411-1960f8a560a5.json")
    spark.conf.set("parentProject", "resounding-keel-378411")

    logger.info("Spark session initialized successfully")

    return spark


@task(name="Generate LSH Parameters")
def generate_lsh_parameters(
    threshold: float,
    num_perm: int,
    b: int = None,
    r: int = None,
    false_positive_weight: float = 0.5,
    false_negative_weight: float = 0.5,
) -> Dict[str, Any]:
    """
    Generate LSH parameters for MinHash algorithm.

    Parameters
    ----------
    threshold : float
        The similarity threshold.
    num_perm : int
        The number of permutations.
    b : int, optional
        The number of bands.
    r : int, optional
        The number of rows per band.
    false_positive_weight : float, optional
        The weight of false positive.
    false_negative_weight : float, optional
        The weight of false negative.

    Returns
    -------
    Dict[str, Any]
        A dictionary containing the LSH parameters.
    """
    logger = get_run_logger()

    def optimal_param(
        threshold: float,
        num_perm: int,
        false_positive_weight: float = 0.5,
        false_negative_weight: float = 0.5,
    ):
        """
        Compute the optimal `MinHashLSH` parameter that minimizes the weighted sum
        of probabilities of false positive and false negative, taken from datasketch.
        """

        def false_positive_probability(threshold: float, b: int, r: int):
            """Source: `datasketch.lsh`"""

            def proba(s):
                return 1 - (1 - s ** float(r)) ** float(b)

            a, _ = integrate(proba, 0.0, threshold)
            return a

        def false_negative_probability(threshold: float, b: int, r: int):
            """Source: `datasketch.lsh`"""

            def proba(s):
                return 1 - (1 - (1 - s ** float(r)) ** float(b))

            a, _ = integrate(proba, threshold, 1.0)
            return a

        min_error = float("inf")
        opt = (0, 0)
        for b in range(1, num_perm + 1):
            max_r = int(num_perm / b)
            for r in range(1, max_r + 1):
                fp = false_positive_probability(threshold, b, r)
                fn = false_negative_probability(threshold, b, r)
                error = fp * false_positive_weight + fn * false_negative_weight
                if error < min_error:
                    min_error = error
                    opt = (b, r)
        return opt

    # If b and r are not provided, compute optimal parameters
    if b is None or r is None:
        B, R = optimal_param(threshold, num_perm, false_positive_weight, false_negative_weight)
        logger.info(f"Using optimal parameters: B={B}, R={R}")
    else:
        B, R = b, r
        logger.info(f"Using provided parameters: B={B}, R={R}")

    # Generate hash ranges
    hash_ranges = [(i * R, (i + 1) * R) for i in range(B)]

    # Generate permutations
    permutations = np.array(
        [
            (
                RNG.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                RNG.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(num_perm)
        ],
        dtype=np.uint64,
    ).T

    return {
        "B": B,
        "R": R,
        "hash_ranges": hash_ranges,
        "permutations": permutations,
    }


@task(
    name="Load Data from BigQuery",
    description="Extract data from source",
    retries=3,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
    tags=["extract", "data-pipeline"],
)
def load_data_from_bigquery(table: str, column: str) -> Tuple:
    """
    Load data from a BigQuery table.

    Parameters
    ----------
    spark : SparkSession
        The Spark session.
    table : str
        The BigQuery table to load data from.

    Returns
    -------
    Tuple
        A tuple containing the DataFrame and the RDD of records.
    """
    logger = get_run_logger()
    logger.info(f"Loading data from BigQuery table: {table}")

    spark = init_spark_session()

    df = spark.read.format("bigquery").option("table", table).option("viewsEnabled", "true").load()
    df = df.withColumn("__id__", F.monotonically_increasing_id()).cache()

    records = df.select("__id__", column).rdd
    num_records = records.count()
    logger.info(f"Loaded {num_records} records from BigQuery")

    store_in_gcs(df, "task=load_data_from_bigquery/bq_dataframe")


@task(
    name="Repartition Records",
    description="Repartition the data",
    tags=["transform", "data-pipeline"],
)
def repartition_records(num_perm: int, column: str):
    """
    Repartition the records for better parallelism.

    Parameters
    ----------
    num_perm : int
        The number of permutations.
    column : str
        The column to deduplicate.

    Returns
    -------
    RDD
        The repartitioned RDD.
    """
    logger = get_run_logger()
    logger.info(f"Repartitioning records with {num_perm * 2} partitions")

    spark = init_spark_session()

    df = read_from_gcs(spark, "task=load_data_from_bigquery/bq_dataframe")
    records = df.select("__id__", column).rdd

    return records.repartition(num_perm * 2).cache()


@task(
    name="Generate Hash Values",
    description="Generate Hash Values",
    tags=["transform", "data-pipeline"],
)
def create_hash_value_generator(
    ngram_size: int,
    min_ngram_size: int,
    num_perm: int,
    hash_ranges: List[Tuple[int, int]],
    permutations: np.ndarray,
):
    """
    Create a function to generate hash values for documents.

    Parameters
    ----------
    ngram_size : int
        The size of the n-grams.
    min_ngram_size : int
        The minimum number of items in the sequence to generate n-grams.
    num_perm : int
        The number of permutations.
    hash_ranges : List[Tuple[int, int]]
        The ranges of offsets for each hash value.
    permutations : np.ndarray
        The permutations for the hash values.

    Returns
    -------
    function
        A function to generate hash values for documents.
    """
    logger = get_run_logger()
    logger.info("Creating hash value generator function")

    def ngrams(sequence: List[str], n: int, min_size: int = 5) -> Iterable:
        """
        Generate n-grams from a sequence of items.
        """
        if len(sequence) < min_size:
            return []

        iterables = tee(sequence, n)
        for i, sub_iterable in enumerate(iterables):
            for _ in range(i):
                next(sub_iterable, None)
        return zip(*iterables)

    def sha1_hash32(data):
        """
        Compute the first 4 bytes (32 bits) of the SHA1 hash of the input data.
        """
        return struct.unpack("<I", hashlib.sha1(data).digest()[:4])[0]

    def generate_hash_values(
        content: str,
        idx: int,
    ) -> List[Tuple[int, bytes, int]]:
        """
        Generate the MinHashLSH values for a given document.
        """
        hashvalues = np.ones(num_perm, dtype=np.uint64) * MAX_HASH
        tokens = {" ".join(t) for t in ngrams(NON_ALPHA.split(content), ngram_size, min_ngram_size)}

        if not tokens:
            return []

        hv = np.array([sha1_hash32(token.encode("utf-8")) for token in tokens], dtype=np.uint64)
        a, b = permutations
        phv = np.bitwise_and(((hv * np.tile(a, (len(hv), 1)).T).T + b) % MERSENNE_PRIME, MAX_HASH)
        hashvalues = np.vstack([phv, hashvalues]).min(axis=0)
        Hs = [bytes(hashvalues[start:end].byteswap().data) for start, end in hash_ranges]

        return [(band_idx, H, idx) for band_idx, H in enumerate(Hs)]

    return generate_hash_values


@task(name="Generate Edges")
def create_edge_generator():
    """
    Create a function to generate edges from clusters.

    Returns
    -------
    function
        A function to generate edges from clusters.
    """
    logger = get_run_logger()
    logger.info("Creating edge generator function")

    def generate_edges(nodes: List[int]) -> List[Tuple[int, int]]:
        """
        Generate edges from a cluster.
        """
        if len(nodes) <= 1:
            return []

        min_node = min(nodes)
        return [(n, min_node) for n in nodes if n != min_node]

    return generate_edges


@task(name="Find Connected Components")
def find_connected_components(max_iterations: int = 100):
    """
    Find connected components in the graph.

    Parameters
    ----------
    max_iterations : int, optional
        The maximum number of iterations.

    Returns
    -------
    list
        The list of connected components.
    """
    logger = get_run_logger()
    logger.info("Finding connected components")

    def large_star_map(edge):
        return [(edge[0], edge[1]), (edge[1], edge[0])]

    def large_star_reduce(group):
        x, neighbors = group
        nodes = [x] + list(neighbors)
        minimum = min(nodes)
        return [(n, minimum) for n in nodes if n > x]

    def small_star_map(edge):
        x, y = edge
        if y <= x:
            return (x, y)
        else:
            return (y, x)

    def small_star_reduce(group):
        x, neighbors = group
        nodes = [x] + list(neighbors)
        minimum = min(nodes)
        return [(n, minimum) for n in nodes if n != minimum]

    spark = init_spark_session()
    edges = read_from_gcs(spark, "task=generate_edges/edges_df").rdd
    a = edges
    iteration = 0

    while iteration < max_iterations:
        iteration += 1
        logger.info(f"Connected components iteration: {iteration}")

        b = a.flatMap(large_star_map).groupByKey().flatMap(large_star_reduce).distinct().cache()
        a = b.map(small_star_map).groupByKey().flatMap(small_star_reduce).distinct().cache()

        changes = a.subtract(b).union(b.subtract(a)).collect()
        if len(changes) == 0:
            logger.info(f"Connected components converged after {iteration} iterations")
            break

    results = a.collect()
    logger.info(f"Found {len(results)} connected components")

    return results


@task(name="Generate Edges")
def generate_edges(num_perm, column, hash_value_generator, edge_generator):
    logger = get_run_logger()
    logger.info("Generating edges")

    spark = init_spark_session()

    repartitioned_records = repartition_records(num_perm, column)
    edges = (
        repartitioned_records.flatMap(lambda x: hash_value_generator(x[1], x[0]))
        .groupBy(lambda x: (x[0], x[1]))
        .flatMap(lambda x: edge_generator([i[2] for i in x[1]]))
        .distinct()
        .cache()
    )

    schema = StructType(
        [StructField("col1", IntegerType(), True), StructField("col2", IntegerType(), True)]
    )

    edges_df = spark.createDataFrame(edges, schema)
    store_in_gcs(edges_df, "task=generate_edges/edges_df")


@task(name="Remove Duplicates")
def remove_duplicates(results, output: str):
    """
    Remove duplicates from the DataFrame.

    Parameters
    ----------
    results : list
        The list of connected components.
    output : str
        The output directory.

    Returns
    -------
    None
    """
    logger = get_run_logger()

    spark = init_spark_session()
    df = read_from_gcs(spark, "task=load_data_from_bigquery/bq_dataframe")
    if len(results) == 0:
        logger.info("No components found, writing original data to output")
        df.write.option("maxRecordsPerFile", 300_000).option("intermediateFormat", "orc").parquet(
            output, mode="overwrite"
        )
        return

    logger.info(f"Creating components DataFrame with {len(results)} rows")
    components = spark.createDataFrame(results, schema=["__id__", "component"]).sort(
        ["component", "__id__"]
    )

    # Show a sample of the components for debugging
    logger.info("Sample of components:")
    components.show(10)

    # Join with original DataFrame and filter out duplicates
    logger.info("Removing duplicates from original DataFrame")
    df = df.join(components, on="__id__", how="left")
    df = df.filter(F.col("component").isNull()).drop("__id__", "component").cache()

    # Write results to output
    logger.info(f"Writing deduplicated data to {output}")
    df.write.option("maxRecordsPerFile", 300_000).option("intermediateFormat", "orc").parquet(
        output, mode="overwrite"
    )

    logger.info("Deduplication completed successfully")


@task(name="Cleanup Spark Session")
def cleanup_spark():
    spark = init_spark_session()
    spark.stop()


@flow(name="Near-Deduplication Flow")
def near_deduplication_flow(
    table: str,
    output: str,
    threshold: float = 0.7,
    min_ngram_size: int = 5,
    ngram_size: int = 5,
    num_perm: int = 256,
    b=None,
    r=None,
    column: str = "content",
):
    """
    Main flow for near-deduplication.

    Parameters
    ----------
    table : str
        The BigQuery table to deduplicate.
    output : str
        The output directory.
    threshold : float, optional
        The similarity threshold.
    min_ngram_size : int, optional
        The minimum number of items in the sequence to generate n-grams.
    ngram_size : int, optional
        The size of the n-grams.
    num_perm : int, optional
        The number of permutations.
    b : int, optional
        The number of bands.
    r : int, optional
        The number of rows per band.
    column : str, optional
        The column to deduplicate.
    staging_gcs_bucket: str
        The GCS bucket to keep staging files
    staging_dir: str
        The directory to keep staging files

    Returns
    -------
    None
    """
    logger = get_run_logger()
    logger.info(f"Starting near-deduplication flow for table: {table}")

    # Initialize Spark session
    init_spark_session()

    # Generate LSH parameters
    lsh_params = generate_lsh_parameters(threshold, num_perm, b, r)

    # Load data from BigQuery
    load_data_from_bigquery(table, column)

    # Create hash value generator
    hash_value_generator = create_hash_value_generator(
        ngram_size,
        min_ngram_size,
        num_perm,
        lsh_params["hash_ranges"],
        lsh_params["permutations"],
    )

    # Create edge generator
    edge_generator = create_edge_generator()

    # Generate edges
    generate_edges(num_perm, column, hash_value_generator, edge_generator)

    # Find connected components
    results = find_connected_components()

    # # Remove duplicates
    remove_duplicates(results, output)

    logger.info("Near-deduplication flow completed successfully")

    cleanup_spark()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Near-deduplicating BigQuery Table with Prefect and PySpark"
    )

    # Required arguments
    parser.add_argument("--table", type=str, required=True, help="BigQuery table to deduplicate")
    parser.add_argument("--output", "-o", type=str, required=True, help="Output directory")

    # Optional arguments with sensible defaults
    parser.add_argument(
        "--threshold", type=float, default=0.7, help="Similarity threshold (0.0-1.0)"
    )
    parser.add_argument(
        "--min_ngram_size", type=int, default=5, help="Minimum document size to process"
    )
    parser.add_argument("--ngram_size", type=int, default=5, help="N-gram size")
    parser.add_argument("--num_perm", type=int, default=256, help="Number of permutations")
    parser.add_argument(
        "--b", type=int, default=None, help="Number of bands (if None, computed optimally)"
    )
    parser.add_argument(
        "--r", type=int, default=None, help="Number of rows per band (if None, computed optimally)"
    )
    parser.add_argument("--column", "-c", type=str, default="content", help="Column to deduplicate")
    parser.add_argument("--staging_gcs_bucket", type=str, help="GCS bucket for staging files")
    parser.add_argument("--staging_dir", type=str, default="", help="Directory for staging files")

    args = parser.parse_args()

    Variable.set("staging_gcs_bucket", args.staging_gcs_bucket, overwrite=True)
    Variable.set("staging_dir", args.staging_dir, overwrite=True)

    # Run the flow
    near_deduplication_flow(
        table=args.table,
        output=args.output,
        threshold=args.threshold,
        min_ngram_size=args.min_ngram_size,
        ngram_size=args.ngram_size,
        num_perm=args.num_perm,
        b=args.b,
        r=args.r,
        column=args.column,
    )
