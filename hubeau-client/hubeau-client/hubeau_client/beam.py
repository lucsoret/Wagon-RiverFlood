import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import json
from datetime import datetime, timedelta, timezone
from google.cloud import storage

client = storage.Client()


class FetchFromAPI(beam.DoFn):
    def process(self, element):
        lasthour = datetime.now(timezone.utc) - timedelta(hours=1)
        print( lasthour.isoformat().replace("+00:00", "Z"))
        date_filter = lasthour.isoformat().replace("+00:00", "Z")
        url = f"https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_entite=O972001001&date_debut_obs={date_filter}"
        print(url)
        #url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_entite=K*"
        response = requests.get(url)
        print(response.status_code)
        if response.status_code >= 200 & response.status_code < 300 :
            yield json.loads(response.text)
        else:
            yield None

def sendToGcp(temp_file_path):
    print(temp_file_path)

    bucket_name = "riverflood-lewagon-dev"

        # data  | 'Print in Terminal 2' >> beam.Map(print)
    today = datetime.today()
    year, month, day, hour = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d"), today.strftime("%H")
    target_gcs_path = f"datasets/raw/diary-entries/{year}/{month}/{day}/observation-{hour}.json"
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(target_gcs_path)
    blob.upload_from_filename(temp_file_path)

def run(
    interval=15
):
    options = beam.options.pipeline_options.PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:

        # data section
        ids = [1]  # Example IDs
        temp_file_path = "tempFile"
        data = (
            pipeline
            | 'Create IDs' >> beam.Create(ids)
            | 'Fetch Data from API' >> beam.ParDo(FetchFromAPI())
            #| 'Print Results' >> beam.Map(print)


        )

        out = (
            data
            | 'write output' >> beam.io.textio.WriteToText(temp_file_path, file_name_suffix='.json')
            | 'Send Results' >> beam.Map(sendToGcp)
        )
        #            | 'Print Results' >> beam.Map(print)

        #bucket_name = "riverflood-lewagon-dev"

        # data  | 'Print in Terminal 2' >> beam.Map(print)
        #today = datetime.today()
        #year, month, day = today.strftime("%Y"), today.strftime("%m"), today.strftime("%d")
        #target_gcs_path = f"datasets/raw/diary-entries/{year}/{month}/{day}/observation.json"
        #bucket = client.get_bucket(bucket_name)
        #blob = bucket.blob(target_gcs_path)
        #blob.upload_from_filename(temp_file_path)

if __name__ == "__main__":

    run(

    )
