#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime


def processline(line):
    # Add your code to filter/extract/etc
    yield line

def processline2(line):
    # Add your code to filter/extract/etc
    yield line

def run():
    projectname = os.getenv('GOOGLE_CLOUD_PROJECT')

    argv = [
        '--runner=DirectRunner',
    ]

    p = beam.Pipeline(argv=argv)
    input = 'gs://roigcp-beam-data/samples/*.csv'
    output = 'output/output'
    output2 = 'output2/output'
    
    lines = p | 'Read Files' >> beam.io.ReadFromText(input)

    (lines 
     | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
#     | 'Reducer' >> beam.   (ADD A REDUCER)
     | 'Write Output' >> beam.io.WriteToText(output)
    )

    (lines 
     | 'Process Lines 2' >> beam.FlatMap(lambda line: processline2(line))
#     | 'Reducer' >> beam.   (ADD A SECOND REDUCER)
     | 'Write Second Output' >> beam.io.WriteToText(output2)
    )

    p.run()


if __name__ == '__main__':
    run()
