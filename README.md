# Solution K-Stack Project

This is a PySpark application for finding and removing near-duplicate documents in a BigQuery table. It uses MinHash LSH to efficiently identify potentially similar documents, then applies a connected components algorithm to cluster them.
## Key Components
1. MinHash LSH Implementation:
    * Converts documents into n-grams
    * Hashes these n-grams and applies permutations to create signatures
    * Groups similar documents based on signature bands
2. Connected Components Algorithm:
   * Uses the "Hash-to-Min" algorithm with alternating "Large Star" and "Small Star" steps
   * Efficiently clusters documents in a distributed way
3. Main Workflow:
   * Reads data from BigQuery
   * Processes documents into MinHash signatures
   * Groups similar documents via LSH
   * Forms edges between similar documents
   * Runs connected components to find clusters
   * Removes duplicates by keeping one document from each cluster

## Architecture

The project utilizes:

* **Prefect:** For workflow orchestration, scheduling, and monitoring.
* **PySpark:** For distributed data processing and transformation.
* **Google BigQuery:** As the initial data source.
* **Google Cloud Storage (GCS):** As the persistent storage layer.
* **Delta Lake:** For efficient and reliable data storage in GCS.

## Repository Structure

    solution-kstack/
        ├── src/
        │   ├── minhash_deduplication_flow.py  # Main Prefect flow and tasks
        ├── venv/                             # Python virtual environment (if present)
        ├── requirements.txt                  # Python dependencies
        └── tools/
        │   ├── install_cert.py
        │   ├── upload_bq.py
        └── sql/
        │   └── kstack_combined_view.sql
        │   └── schema.sql
        │── .pre-commit-config.yaml
        │── .pylintrc
        │── Makefile
        │── README.md

## Prerequisites

Before running the project, ensure you have the following:

* Python 3.11
* Apache Spark 3.5.5
* Prefect 2.x
* Google Cloud SDK (gcloud) configured with access to BigQuery and GCS.
* A Google Cloud project with BigQuery and GCS enabled.
* Delta Lake library.

## Installation

1.  **Clone the repository:**

    ```bash
    git clone [https://github.com/Tirayr/solution-kstack.git](https://www.google.com/search?q=https://github.com/Tirayr/solution-kstack.git)
    cd solution-kstack
    ```

2.  **Create and activate a virtual environment (recommended):**

    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On macOS/Linux
    venv\Scripts\activate      # On Windows
    ```

3.  **Install dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Google Cloud credentials:**

    ```bash
    gcloud auth login
    gcloud config set project YOUR_GOOGLE_CLOUD_PROJECT_ID
    ```

5.  **Configure Spark:**
    * Make sure that spark is installed, and that the delta lake jars are correctly included.

## Running the Pipeline

To execute the Prefect flow, run the following command from the `src` directory:

```bash
python minhash_deduplication_flow.py --table resounding-keel-378411:kstack.kstack_combined_view 
--output "gs://kstack_output/data/"
--min_ngram_size 5 
--ngram_size 5 
--threshold 0.7 
--staging_gcs_bucket gs://kstack_output 
--staging_dir staging_file
```
