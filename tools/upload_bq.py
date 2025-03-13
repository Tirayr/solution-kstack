import pandas as pd
from datasets import load_dataset
from google.cloud import bigquery
from google.oauth2 import service_account
import json


def load_kstack_and_upload_to_bq(project_id, dataset_id):
    """Loads the KStack dataset from Hugging Face and uploads it to BigQuery."""

    try:
        # Load the KStack dataset
        dataset = load_dataset("JetBrains/KStack")
        print("Dataset loaded from initial source")

        # Access the 'train' split
        df = dataset['train'].to_pandas()

        # Get credentials from json
        credentials = service_account.Credentials.from_service_account_file(
            "resounding-keel-378411-1960f8a560a5.json",
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        # BigQuery Client
        client = bigquery.Client(project=project_id, credentials=credentials)
        dataset_ref = client.dataset(dataset_id)

        # Languages Dimension Table
        languages = df['main_language'].fillna("N/A").unique()
        languages_df = pd.DataFrame({
            'language_id': range(1, len(languages) + 1),
            'language_name': languages
        })
        languages_job = client.load_table_from_dataframe(
            languages_df, dataset_ref.table('Languages'),
            job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
        )
        languages_job.result()  # Wait for the job to complete
        print("Languages data loaded to BigQuery.")

        # Create language map for efficient lookups
        language_map = dict(zip(languages, range(1, len(languages) + 1)))

        # Repositories Dimension Table
        repos = df.drop_duplicates(subset=['repo_id'])
        repos_df = pd.DataFrame({
            'repo_id': repos['repo_id'],
            'language_id': repos['main_language'].fillna("N/A").map(language_map),
            'owner': repos['owner'],
            'name': repos['name'],
            'is_fork': repos['is_fork'],
            'forks': repos['forks'],
            'stars': repos['stars'],
            'issues': repos['issues'],
            'license': repos['license']
        })
        repos_job = client.load_table_from_dataframe(
            repos_df, dataset_ref.table('Repositories'),
            job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
        )
        repos_job.result()
        print("Repositories data loaded to BigQuery.")

        # Files Fact Table
        files_df = pd.DataFrame({
            'file_id': range(1, len(df) + 1),
            'repo_id': df['repo_id'],
            'path': df['path'],
            'content': df['content'],
            'size': df['size'],
            'commit_sha': df['commit_sha'],
            'languages_distribution': df['languages_distribution'].apply(lambda x: json.dumps(x)) #convert dict to json string.
        })

        files_job = client.load_table_from_dataframe(
            files_df, dataset_ref.table('Files'),
            job_config=bigquery.LoadJobConfig(write_disposition='WRITE_TRUNCATE')
        )
        files_job.result()
        print("Files data loaded to BigQuery.")

        print("KStack data loaded to BigQuery successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    project_id = "resounding-keel-378411"
    dataset_id = "kstack"
    load_kstack_and_upload_to_bq(project_id, dataset_id)