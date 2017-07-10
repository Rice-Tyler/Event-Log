from __future__ import absolute_import
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
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
      anonymous_id = attr_dict[""].strip()
      context_1 = attr_dict[""].strip()
      context_2 = attr_dict[""].strip()
      context_3 = attr_dict[""].strip()
      context_4 = attr_dict[""].strip()
      event = attr_dict[""].strip()
      event_text = attr_dict[""].strip()
      id = attr_dict[""].strip()
      sent_at = attr_dict[""].strip()
      received_at = attr_dict[""].strip()
      user_id = attr_dict[""].strip()
      yield {'anonymous_id': anonymous_id, 'context_1': context_1, 'context_2': context_2,
             'context_3' : context_3, 'context_4' : context_4, 'event':event,
             'event_text' : event_text,'id': id,'sent_at' : sent_at,'received_at':received_at,
             'user_id' : user_id}
    except:  # pylint: disable=bare-except
      # Log and count parse errors.
      self.num_parse_errors.inc()
      logging.info('Parse error on %s.', element)

def configure_bigquery_write():
  return [
      ('anonymous_id', 'STRING', lambda e: e[0]),
      ('context_1', 'STRING', lambda e: e[1]), ##Context field placeholders v
      ('context_2', 'STRING', lambda e: e[2]),
      ('context_3', 'STRING', lambda e: e[3]),
      ('context_4', 'STRING', lambda e: e[4]),##Context field placeholders ^^
      ('event', 'STRING', lambda e: e[1]),
      ('event_text', 'STRING', lambda e: e[1]),
      ('id', 'STRING', lambda e: e[1]),
      ('received_at', 'STRING', lambda e: e[1]),
      ('sent_at', 'STRING', lambda e: e[1]),
      ('user_id', 'STRING', lambda e: e[1]),
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


def run(argv=None):
  """Main entry point; defines and runs the user_score pipeline."""
  parser = argparse.ArgumentParser()

  # The default maps to two large Google Cloud Storage files (each ~12GB)
  # holding two subsequent day's worth (roughly) of data.
  parser.add_argument('--input',
                      dest='input',
                      default='SOURCE NAME HERE',
                      help='Path to the data file(s)')
  parser.add_argument('--dataset',
                      dest='dataset',
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                           'Must already exist.')
  parser.add_argument('--table_name',
                      dest='table_name',
                      default='user_score',
                      help='The BigQuery table name. Should not already exist.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:

    (p  # pylint: disable=expression-not-assigned
     | ReadFromText(known_args.input) # Read events from a file and parse them.
     | ########INSERT TRANSFORM ##########
     | WriteToBigQuery(
         known_args.table_name, known_args.dataset, configure_bigquery_write()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()