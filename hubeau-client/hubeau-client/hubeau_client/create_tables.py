#from pipeline.schemas import bigquery_schemas
from google.cloud import bigquery
#from google.cloud.exceptions import NotFound
import os


def main():
    """
    Main function to take the bigquery schemas and create the tables in the raw dataset.
    """
    client = bigquery.Client()
    dataset_name = os.environ.get("BIGQUERY_DATASET")
    print("dataset_name", dataset_name)
    dataset_ref = client.dataset(dataset_name)
    print(dataset_ref)



if __name__ == "__main__":
    main()
