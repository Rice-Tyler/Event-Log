from __future__ import absolute_import

import json
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import typehints
from apache_beam.metrics import Metrics
from apache_beam.typehints import with_input_types
from apache_beam.typehints import with_output_types
from apache_beam.options.pipeline_options import GoogleCloudOptions


class ParseEventFn(beam.DoFn):
  """Parses the raw event info into tuples.
  Each event line comes in a JSON format with the following order

  {app,device_carrier,$schema,city,user_id,uuid,event_time,platform,os_version,amplitude_id,
  processed_time,user_creation_time,version_name,ip_address,paying,dma,group_properties:{},
  user_properties:{project_count,photos_permission,push_channels,icloud_sync_enabled:{},
  client_upload_time,$insert_id,event_type,library,device_type,device_manufacturer,
  start_version,location_lng,server_upload_time,event_id,location_lat,os_name,amplitude_event_type,
  device_brand,groups,event_properties,data,device_id,language,device_model,country,region,adid,
  session_id,device_family,sample_rate,idfa,client_event_time}


  """
  def __init__(self):
    super(ParseEventFn, self).__init__()
    self.num_parse_errors = Metrics.counter(self.__class__, 'num_parse_errors')

  def process(self, element):
      attr = element.strip('{').strip("}").replace('"', "").split(",")
      if (attr[15].split(":")[1] != "null"):
          attr[15] = attr[15] + attr[16]
          attr.remove(attr[16])
      attr_dict = {}
      for k in attr:
          print(k)
          key_val = k.split(":", 1)
          print(key_val)
          attr_dict[key_val[0]] = key_val[1]

    try:
      user = components[0].strip()
      team = components[1].strip()
      score = int(components[2].strip())
      timestamp = int(components[3].strip())
      yield {'user': user, 'team': team, 'score': score, 'timestamp': timestamp}
    except:  # pylint: disable=bare-except
      # Log and count parse errors.
      self.num_parse_errors.inc()
      logging.info('Parse error on %s.', element)

def configure_bigquery_write():
  return [
      ('user_id', 'STRING', lambda e: e[0]),
      ('total_score', 'INTEGER', lambda e: e[1]),
  ]


class WriteToBigQuery(beam.PTransform):
  """Generate, format, and write BigQuery table row information.
  Use provided information about the field names and types, as well as lambda
  functions that describe how to generate their values.
  """

  def __init__(self, table_name, dataset, field_info):
    """Initializes the transform.
    Args:
      table_name: Name of the BigQuery table to use.
      dataset: Name of the dataset to use.
      field_info: List of tuples that holds information about output table field
                  definitions. The tuples are in the
                  (field_name, field_type, field_fn) format, where field_name is
                  the name of the field, field_type is the BigQuery type of the
                  field and field_fn is a lambda function to generate the field
                  value from the element.
    """
    super(WriteToBigQuery, self).__init__()
    self.table_name = table_name
    self.dataset = dataset
    self.field_info = field_info

  def get_schema(self):
    """Build the output table schema."""
    return ', '.join(
        '%s:%s' % (entry[0], entry[1]) for entry in self.field_info)

  def get_table(self, pipeline):
    """Utility to construct an output table reference."""
    project = pipeline.options.view_as(GoogleCloudOptions).project
    return '%s:%s.%s' % (project, self.dataset, self.table_name)

  class BuildRowFn(beam.DoFn):
    """Convert each key/score pair into a BigQuery TableRow as specified."""
    def __init__(self, field_info):
      super(WriteToBigQuery.BuildRowFn, self).__init__()
      self.field_info = field_info

    def process(self, element):
      row = {}
      for entry in self.field_info:
        row[entry[0]] = entry[2](element)
      yield row

  def expand(self, pcoll):
    table = self.get_table(pcoll.pipeline)
    return (
        pcoll
        | 'ConvertToRow' >> beam.ParDo(
            WriteToBigQuery.BuildRowFn(self.field_info))
        | beam.io.Write(beam.io.BigQuerySink(
            table,
            schema=self.get_schema(),
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND)))





def run(argv = None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dests = 'input',
                        default = 'C:/Users/Student/Documents/Over/146509/146509_2017-06-28_18#649.json.gz',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        default='',
                        help='Output file to write results to.')
    knoown_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner-DataflowRunner',
        'project=SET_PROJ_ID'
    ])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:




