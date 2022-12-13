#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime


def processline(line):
    # Add your code to filter/extract/etc
    yield line

def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')
    bucketname = os.getenv('GOOGLE_CLOUD_PROJECT') + '-bucket'
    jobname = 'dataflow-job' + datetime.datetime.now().strftime("%Y%m%d%H%M")
    region = 'us-central1'

    argv = [
        '--runner=DataflowRunner',
        '--project=' + projectname,
        '--region=' + region,
        '--job_name=' + jobname,
        '--temp_location=gs://' + bucketname + '/temp/',
        '--staging_location=gs://' + bucketname + '/staging/',
	    '--save_main_session'
    ]

    p = beam.Pipeline(argv=argv)
    input = 'gs://roigcp-beam-data/samples/*.csv'
    # input = 'gs://roigcp-beam-data/bankprod/*.csv'
    output = 'gs://' + bucketname + '/output/output'
    
    (p
     | 'Read Files' >> beam.io.ReadFromText(input)
     | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
#     | 'Reducer' >> beam.   (ADD A REDUCER)
     | 'Write Output' >> beam.io.WriteToText(output)
     )
    p.run()


if __name__ == '__main__':
    run()
