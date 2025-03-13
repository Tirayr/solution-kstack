import os
import time
import uuid
from typing import Dict
import re

import numpy as np
from prefect import task, flow, get_run_logger
import google.cloud.dataproc_v1 as dataproc
from google.cloud import storage

# Constants
SEED = 42
NON_ALPHA = re.compile("[^A-Za-z_0-9]")
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)
GCS_BUCKET = "kstack_output"
PROJECT_ID = "resounding-keel-378411"  # os.environ.get("GCP_PROJECT")
REGION = "us-central1"  # os.environ.get("GCP_REGION", "us-central1")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'resounding-keel-378411-1960f8a560a5.json'

@task(name="Create Dataproc Cluster")
def create_dataproc_cluster(cluster_name: str = None):
    """
    Create a Dataproc cluster for running the PySpark application.

    Parameters
    ----------
    cluster_name : str, optional
        The name of the cluster to create. If not provided, a unique name will be generated.

    Returns
    -------
    str
        The name of the created cluster.
    """
    logger = get_run_logger()
    
    if cluster_name is None:
        cluster_name = f"dedup-cluster-{uuid.uuid4().hex[:8]}"
    
    logger.info(f"Creating Dataproc cluster: {cluster_name} in {REGION}")
    
    # Create the Dataproc client
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    
    # Configure cluster
    cluster_config = {
        "project_id": PROJECT_ID,
        "cluster_name": cluster_name,
        "config": {
            "master_config": {
                "num_instances": 1,
                "machine_type_uri": "n2-standard-4",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 500,
                },
            },
            "worker_config": {
                "num_instances": 2,
                "machine_type_uri": "n2-standard-8",
                "disk_config": {
                    "boot_disk_type": "pd-standard",
                    "boot_disk_size_gb": 500,
                },
            },
            "software_config": {
                "image_version": "2.2.49-debian12",
                "optional_components": [
                    "JUPYTER",
                    "DOCKER"
                ],
                "properties": {
                    "spark:spark.executor.memory": "10g",
                    "spark:spark.driver.memory": "4g",
                    "spark:spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.27.1," +
                    "io.delta:delta-spark_2.12:3.3.0",
                    "spark:spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                    "spark:spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"
                }
            },
            "initialization_actions": [
                {
                    "executable_file": "gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh",
                }
            ],
            'gce_cluster_config': {
                "metadata": {
                    "PIP_PACKAGES": "aiohappyeyeballs==2.5.0 aiohttp==3.11.13 aiosignal==1.3.2 aiosqlite==0.21.0 alembic==1.15.1 annotated-types==0.7.0 anyio==4.8.0 apprise==1.9.2 asgi-lifespan==2.1.0 asyncpg==0.30.0 attrs==25.1.0 cachetools==5.5.2 certifi==2025.1.31 cffi==1.17.1 charset-normalizer==3.4.1 click==8.1.8 cloudpickle==3.1.1 colorama==0.4.6 coolname==2.2.0 cryptography==44.0.2 datasets==3.3.2 dateparser==1.2.1 db-dtypes==1.4.2 Deprecated==1.2.18 dill==0.3.8 docker==7.1.0 exceptiongroup==1.2.2 fastapi==0.115.11 filelock==3.17.0 frozenlist==1.5.0 fsspec==2024.12.0 google-api-core==2.24.1 google-auth==2.38.0 google-auth-oauthlib==1.2.1 google-cloud-bigquery==3.30.0 google-cloud-core==2.4.2 google-cloud-dataproc==5.18.0 google-cloud-storage==3.1.0 google-crc32c==1.6.0 google-resumable-media==2.7.2 googleapis-common-protos==1.69.1 graphviz==0.20.3 greenlet==3.1.1 griffe==1.6.0 grpc-google-iam-v1==0.14.1 grpcio==1.71.0rc2 grpcio-status==1.71.0rc2 h11==0.14.0 h2==4.2.0 hpack==4.1.0 httpcore==1.0.7 httpx==0.28.1 huggingface-hub==0.29.2 humanize==4.12.1 hyperframe==6.1.0 idna==3.10 importlib_metadata==8.5.0 Jinja2==3.1.6 jinja2-humanize-extension==0.4.0 jsonpatch==1.33 jsonpointer==3.0.0 jsonschema==4.23.0 jsonschema-specifications==2024.10.1 Mako==1.3.9 Markdown==3.7 markdown-it-py==3.0.0 MarkupSafe==3.0.2 mdurl==0.1.2 multidict==6.1.0 multiprocess==0.70.16 numpy==2.2.3 oauthlib==3.2.2 opentelemetry-api==1.30.0 orjson==3.10.15 packaging==24.2 pandas==2.2.3 pandas-gbq==0.28.0 pathspec==0.12.1 pendulum==3.0.0 prefect==3.2.11 prometheus_client==0.21.1 propcache==0.3.0 proto-plus==1.26.0 protobuf==5.29.3 psutil==7.0.0 py4j==0.10.9.7 pyarrow==19.0.1 pyasn1==0.6.1 pyasn1_modules==0.4.1 pycparser==2.22 pydantic==2.10.6 pydantic-extra-types==2.10.2 pydantic-settings==2.8.1 pydantic_core==2.27.2 pydata-google-auth==1.9.1 Pygments==2.19.1 pyspark==3.5.5 python-dateutil==2.9.0.post0 python-dotenv==1.0.1 python-slugify==8.0.4 python-socks==2.7.1 pytz==2025.1 PyYAML==6.0.2 readchar==4.2.1 referencing==0.36.2 regex==2024.11.6 requests==2.32.3 requests-oauthlib==2.0.0 rfc3339-validator==0.1.4 rich==13.9.4 rpds-py==0.23.1 rsa==4.9 ruamel.yaml==0.18.10 scipy==1.15.2 setuptools==75.8.2 shellingham==1.5.4 six==1.17.0 sniffio==1.3.1 SQLAlchemy==2.0.38 starlette==0.46.1 text-unidecode==1.3 time-machine==2.16.0 toml==0.10.2 tqdm==4.67.1 typer==0.15.2 typing_extensions==4.12.2 tzdata==2025.1 tzlocal==5.3.1 ujson==5.10.0 urllib3==2.3.0 uv==0.6.5 uvicorn==0.34.0 websockets==15.0.1 wrapt==1.17.2 xxhash==3.5.0 yarl==1.18.3 zipp==3.21.0"
            }
            }
        }
    }
    
    # Create the cluster
    operation = client.create_cluster(
        request={"project_id": PROJECT_ID, "region": REGION, "cluster": cluster_config}
    )
    
    logger.info("Waiting for cluster creation operation to complete...")
    operation.result()
    
    logger.info(f"Cluster {cluster_name} created successfully")
    return cluster_name


@task(name="Ensure GCS Bucket Exists")
def ensure_bucket_exists(bucket_name: str = GCS_BUCKET):
    """
    Ensure the GCS bucket exists, creating it if it doesn't.

    Parameters
    ----------
    bucket_name : str, optional
        The name of the bucket to ensure exists.

    Returns
    -------
    str
        The name of the bucket.
    """
    logger = get_run_logger()
    logger.info(f"Ensuring GCS bucket exists: {bucket_name}")
    
    storage_client = storage.Client()
    
    # Check if bucket exists
    if not storage_client.bucket(bucket_name).exists():
        logger.info(f"Bucket {bucket_name} does not exist. Creating...")
        bucket = storage_client.create_bucket(bucket_name, location=REGION)
        logger.info(f"Bucket {bucket_name} created in {bucket.location}")
    else:
        logger.info(f"Bucket {bucket_name} already exists")
    
    return bucket_name


@task(name="Upload Code to GCS")
def upload_code_to_gcs(bucket_name: str = GCS_BUCKET):
    """
    Upload the current Python script to GCS for submission to Dataproc.

    Parameters
    ----------
    bucket_name : str, optional
        The name of the bucket to upload to.

    Returns
    -------
    str
        The GCS URI of the uploaded script.
    """
    logger = get_run_logger()
    
    # Get the current script path
    script_path = "/Users/tkarapetyan/gitProjects/solution-kstack/src/minhash_deduplication_flow.py" # os.path.abspath(__file__)
    script_name = os.path.basename(script_path)
    
    # Create a unique file name to avoid conflicts
    timestamp = int(time.time())
    gcs_script_name = f"scripts/{script_name.split('.')[0]}_{timestamp}.py"
    
    logger.info(f"Uploading script {script_path} to gs://{bucket_name}/{gcs_script_name}")
    
    # Upload to GCS
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_script_name)
    
    with open(script_path, "rb") as source_file:
        blob.upload_from_file(source_file)
    
    gcs_uri = f"gs://{bucket_name}/{gcs_script_name}"
    logger.info(f"Script uploaded to {gcs_uri}")
    
    return gcs_uri


@task(name="Submit PySpark Job to Dataproc")
def submit_pyspark_job(
    cluster_name: str,
    script_uri: str,
    table: str,
    output: str,
    threshold: float = 0.7,
    min_ngram_size: int = 5,
    ngram_size: int = 5,
    num_perm: int = 256,
    b: int = None,
    r: int = None,
    column: str = "content",
):
    """
    Submit the PySpark job to the Dataproc cluster.

    Parameters
    ----------
    cluster_name : str
        The name of the Dataproc cluster.
    script_uri : str
        The GCS URI of the script to run.
    table : str
        The BigQuery table to deduplicate.
    output : str
        The output GCS path.
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

    Returns
    -------
    str
        The job ID.
    """
    logger = get_run_logger()
    logger.info(f"Submitting PySpark job to cluster {cluster_name}")
    
    # Create the Dataproc client
    client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    
    # Build job arguments
    args = [
        "--table", table,
        "--output", output,
        "--threshold", str(threshold),
        "--min_ngram_size", str(min_ngram_size),
        "--ngram_size", str(ngram_size),
        "--num_perm", str(num_perm),
        "--column", column,
    ]
    
    if b is not None:
        args.extend(["--b", str(b)])
    if r is not None:
        args.extend(["--r", str(r)])
    
    # Create the job config
    job = {
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": script_uri,
            "args": args,
            "properties": {
                "spark.jars.packages": "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.25.0"
            }
        },
    }
    
    operation = client.submit_job_as_operation(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "job": job,
        }
    )
    
    response = operation.result()
    
    job_id = response.reference.job_id
    logger.info(f"Job {job_id} submitted successfully")
    
    # Monitor job status
    job_status = client.get_job(
        request={"project_id": PROJECT_ID, "region": REGION, "job_id": job_id}
    ).status
    
    logger.info(f"Job {job_id} status: {job_status.state.name}")
    
    return job_id


@task(name="Wait for Job Completion")
def wait_for_job_completion(job_id: str, timeout_seconds: int = 3600):
    """
    Wait for the Dataproc job to complete.

    Parameters
    ----------
    job_id : str
        The job ID to wait for.
    timeout_seconds : int, optional
        The maximum time to wait in seconds.

    Returns
    -------
    bool
        True if the job completed successfully, False otherwise.
    """
    logger = get_run_logger()
    logger.info(f"Waiting for job {job_id} to complete...")
    
    client = dataproc.JobControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    
    start_time = time.time()
    while time.time() - start_time < timeout_seconds:
        job = client.get_job(
            request={"project_id": PROJECT_ID, "region": REGION, "job_id": job_id}
        )
        status = job.status
        
        if status.state.name in ["DONE", "CANCELLED", "ERROR"]:
            logger.info(f"Job {job_id} finished with state: {status.state.name}")
            if status.state.name == "DONE":
                logger.info("Job completed successfully")
                return True
            else:
                logger.error(f"Job failed with details: {status.details}")
                return False
        
        logger.info(f"Job {job_id} is still running. State: {status.state.name}")
        time.sleep(30)  # Check every 30 seconds
    
    logger.error(f"Timeout reached while waiting for job {job_id}")
    return False


@task(name="Delete Dataproc Cluster")
def delete_dataproc_cluster(cluster_name: str):
    """
    Delete the Dataproc cluster.

    Parameters
    ----------
    cluster_name : str
        The name of the cluster to delete.

    Returns
    -------
    bool
        True if deletion was successful, False otherwise.
    """
    logger = get_run_logger()
    logger.info(f"Deleting cluster {cluster_name}")
    
    client = dataproc.ClusterControllerClient(
        client_options={"api_endpoint": f"{REGION}-dataproc.googleapis.com:443"}
    )
    
    operation = client.delete_cluster(
        request={
            "project_id": PROJECT_ID,
            "region": REGION,
            "cluster_name": cluster_name,
        }
    )
    
    try:
        operation.result()
        logger.info(f"Cluster {cluster_name} deleted successfully")
        return True
    except Exception as e:
        logger.error(f"Error deleting cluster {cluster_name}: {e}")
        return False


@flow(name="GCP Dataproc Near-Deduplication Flow")
def gcp_dataproc_deduplication_flow(
        table: str,
        output="gs://kstack_output/data/",
        staging_gcs_bucket="gs://kstack_output",
        staging_dir="staging_files",
        threshold: float = 0.7,
        min_ngram_size: int = 5,
        ngram_size: int = 5,
        num_perm: int = 256,
        b = None,
        r = None,
        column: str = "content",
        keep_cluster: bool = False
):
    """
    Main flow for near-deduplication using GCP Dataproc.

    Parameters
    ----------
    table : str
        The BigQuery table to deduplicate.
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
    keep_cluster : bool, optional
        Whether to keep the Dataproc cluster after job completion.

    Returns
    -------
    Dict
        A dictionary containing the results of the deduplication process.
    """
    logger = get_run_logger()
    logger.info(f"Starting GCP Dataproc near-deduplication flow for table: {table}")
    
    # Ensure GCS bucket exists
    bucket_name = ensure_bucket_exists()
    
    # Define output path
    timestamp = int(time.time())
    # output_path = f"gs://{bucket_name}/deduplication_results_{timestamp}"
    
    # Create Dataproc cluster
    cluster_name = create_dataproc_cluster()
    
    # Upload code to GCS
    script_uri = upload_code_to_gcs(bucket_name)
    
    # Submit job to Dataproc
    job_id = submit_pyspark_job(
        cluster_name=cluster_name,
        script_uri=script_uri,
        table=table,
        output=output,
        staging_gcs_bucket=staging_gcs_bucket,
        staging_dir=staging_dir,
        threshold=threshold,
        min_ngram_size=min_ngram_size,
        ngram_size=ngram_size,
        num_perm=num_perm,
        b=b,
        r=r,
        column=column,
    )
    
    # Wait for job completion
    job_success = wait_for_job_completion(job_id)
    
    # Delete cluster if not keeping it
    if not keep_cluster:
        delete_dataproc_cluster(cluster_name)
    
    # result = {
    #     "job_id": job_id,
    #     "cluster_name": cluster_name,
    #     "job_success": job_success,
    #     "output_path": output_path,
    #     "bucket_name": bucket_name,
    # }
    #
    # logger.info(f"GCP Dataproc near-deduplication flow completed with result: {result}")
    # return result


if __name__ == "__main__":
    gcp_dataproc_deduplication_flow(
        table="resounding-keel-378411:kstack.kstack_combined_view",
        output="gs://kstack_output/data/",
        staging_gcs_bucket="gs://kstack_output",
        staging_dir="staging_files",
        threshold=0.7,
        min_ngram_size=5,
        ngram_size=5,
        num_perm=256,
        b=None,
        r=None,
        column="content",
        keep_cluster=False
    )