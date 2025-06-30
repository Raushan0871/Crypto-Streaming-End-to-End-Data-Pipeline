import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows
import json
import logging
from apache_beam.io import fileio


# Create and configure logger
logging.basicConfig(filename='filelog.log',
                    format='%(asctime)s %(message)s',
                    filemode='w')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

class ParseCryptoJson(beam.DoFn):
    def process(self, element):
        try:
            record = json.loads(element)
            record['tokens'] = json.dumps(record.get('tokens', {}))
            yield {
                
                'id': record.get('id'),
                'rank': int(record.get('rank', 0)),
                'symbol': record.get('symbol'),
                'name': record.get('name'),
                'supply': float(record.get('supply', 0)),
                'maxSupply': float(record.get('maxSupply')) if record.get('maxSupply') else None,
                'marketCapUsd': float(record.get('marketCapUsd', 0)),
                'volumeUsd24Hr': float(record.get('volumeUsd24Hr', 0)),
                'priceUsd': float(record.get('priceUsd', 0)),
                'changePercent24Hr': float(record.get('changePercent24Hr', 0)),
                'vwap24Hr': float(record.get('vwap24Hr', 0)),
                'explorer': record.get('explorer'),
                'tokens': record['tokens']
                
            }
            
        except Exception as e:
            beam.metrics.Metrics.counter('ParseCrptoJson','parse_errors').inc()
            logging.warning(f"Failed to parse: {element[:100]}-- Error: {e}")     
            return []
        
def run(argv=None):
    options = PipelineOptions(
        streaming = True,
        project = 'grounded-region-463501-n1',
        region='us-central1',
        temp_location = 'gs://crypto-api-data/tmp/',
        staging_location = 'gs://crypto-api-data/staging/',
        save_main_session = True  
    )
    
    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            |  'Read from Pub/Sub' >> beam.io.ReadFromPubSub(topic='projects/grounded-region-463501-n1/topics/crypto-data')
            |  'Decode' >> beam.Map(lambda x: x.decode('utf-8'))
            |  'Parse and Transform' >> beam.ParDo(ParseCryptoJson())
        )
        
        # Write to BigQuery

        (
            parsed
            | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                table='grounded-region-463501-n1.crypto_api_data.crypto_data',
                schema={
                    'fields': [
                        {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'rank', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                        {'name': 'symbol', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'supply', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'maxSupply', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'marketCapUsd', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'volumeUsd24Hr', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'priceUsd', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'changePercent24Hr', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'vwap24Hr', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                        {'name': 'explorer', 'type': 'STRING', 'mode': 'NULLABLE'},
                        {'name': 'tokens', 'type': 'STRING', 'mode': 'NULLABLE'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                custom_gcs_temp_location='gs://crypto-api-data/tmp/'
            )
        )

if __name__ == '__main__':
    run()