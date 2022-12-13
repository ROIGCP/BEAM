#!/usr/bin/env python3
import apache_beam as beam
import os
import datetime


def processline(line):
    # Add your code to filter/extract/etc
    yield line


def run():
    argv = [
        '--runner=DirectRunner',
    ]


    p = beam.Pipeline(argv=argv)
    input = 'gs://roigcp-beam-data/samples/*.csv'
    output = 'output/output'


    (p
     | 'Read Files' >> beam.io.ReadFromText(input)
     | 'Process Lines' >> beam.FlatMap(lambda line: processline(line))
#     | 'Reducer' >> beam.   (ADD A REDUCER)
     | 'Write Output' >> beam.io.WriteToText(output)
     )
    p.run()


if __name__ == '__main__':
    run()
