import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import requests
import json

class FetchFromAPI(beam.DoFn):
    def process(self, element):
        url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_entite=O972001001"
        #url = "https://hubeau.eaufrance.fr/api/v1/hydrometrie/observations_tr?code_entite=K*"
        response = requests.get(url)
        print(response.status_code)
        if response.status_code == 206:
            yield json.loads(response.text)
        else:
            yield None


def run(
    interval=15
):
    options = beam.options.pipeline_options.PipelineOptions()

    with beam.Pipeline(options=options) as pipeline:

        # data section
        ids = [1]  # Example IDs

        data = (
            pipeline
            | 'Create IDs' >> beam.Create(ids)
            | 'Fetch Data from API' >> beam.ParDo(FetchFromAPI())
            | 'Print Results' >> beam.Map(print)


        )
        # data  | 'Print in Terminal 2' >> beam.Map(print)




if __name__ == "__main__":

    run(

    )
