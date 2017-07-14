from __future__ import absolute_import
from __future__ import print_function
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
    attr = i.strip('{').strip("}").replace('"', "").split(",")
    ### dma fix
    if (attr[15].split(":")[1] != "null"):
        while (':' not in attr[16]):
            attr[15] = attr[15] + ',' + attr[16]
            attr.remove(attr[16])
    ### JSON sub-group-check
    sub_group_check = 0
    while (sub_group_check < len(attr)):
        try:
            if (len(attr[sub_group_check].split(':')) == 1):
                attr[sub_group_check - 1] = attr[sub_group_check - 1] + "," + attr[sub_group_check]
                attr.remove(attr[sub_group_check])
                sub_group_check -= 1
            sub_group_check += 1
        except:
            self.num_parse_error.inc()
            logging.info('subgroup parse error on %s', i)
    attr_dict = {}
    line_count = 0
    for k in attr:
        line_count += 1
        try:
            if ((len(k.split(':')) > 2) & ("{" in k)):
                key_val = k.replace('{', '', 1).split(':')
                attr_dict[key_val[1]] = key_val[2]
            else:
                key_val = k.strip('\n').split(':')
                attr_dict[key_val[0]] = key_val[1]
        except:
            self.num_parse_error.inc()
            logging.info('split parse error on %s', k)

    try:
      anonymous_id = attr_dict[""].strip(' ')
      context_device_carrier = attr_dict["device_carrier"].strip(" ")
      context_city = attr_dict["city"].strip(" ")
      context_platform = attr_dict["platform"].strip(" ")
      context_os_version = attr_dict["os_version"].strip(" ")
      context_version_name = attr_dict["version_name"].strip(" ")
      context_ip_address = attr_dict["ip_address"].strip(" ")
      context_dma = attr_dict["dma"].strip(" ")
      context_device_type = attr_dict["device_type"].strip(" ")
      context_device_manufacturer = attr_dict["device_manufacturer"].strip(" ")
      context_start_version = attr_dict["start_version"].strip(" ")
      context_os_name = attr_dict["os_name"].strip(" ")
      context_device_id = attr_dict["device_id"].strip(" ")
      context_language = attr_dict["language"].strip(" ")
      context_device_model = attr_dict["device_model"].strip(" ")
      context_country = attr_dict["country"].strip(" ")
      context_region = attr_dict["region"]
      event = attr_dict["event_id"].strip(" ")
      event_text = attr_dict["event_type"].strip(" ")
      id = attr_dict["uuid"].strip(" ")
      sent_at = attr_dict["client_upload_time"].strip(" ")
      received_at = attr_dict["server_upload_time"].strip(" ")
      user_id = attr_dict["user_id"].strip(" ")

      yield {'anonymous_id': anonymous_id, 'context_device_carrier' : context_device_carrier, 'context_city': context_city,
             'context_plstform': context_platform,'context_os_version' : context_os_version, 'context_version_name' : context_version_name,
             'context_ip_address' : context_ip_address, 'context_dma' : context_dma, 'context_device_type' : context_device_type,
             'context_device_manufacturer' : context_device_manufacturer, 'context_start_version' : context_start_version,
             'context_os_name' : context_os_name, 'context_device_id' : context_device_id, 'context_language' : context_language,
             'context_device_model' : context_device_model, 'context_country' : context_country, 'context_region' : context_region,
             'event':event,'event_text' : event_text,'id': id,'sent_at' : sent_at,'received_at':received_at,
             'user_id' : user_id}
    except:  # pylint: disable=bare-except
      # Log and count parse errors.
      self.num_parse_errors.inc()
      logging.info('Parse error on %s.', element)


class Event_Logging(beam.PTransform):
  def expand(self, pcoll):
    return (pcoll
            | 'Parse Event Logs' >> beam.ParDo(ParseEventFn())

            )


def configure_bigquery_write():
  return [
      ('anonymous_id', 'STRING', lambda e: e[0]),
      ('context_device_carrier', 'STRING', lambda e: e[1])
      ('context_city', 'STRING', lambda e: e[2]), ##Context field placeholders v
      ('context_platform', 'STRING', lambda e: e[3]),
      ('context_os_version', 'STRING', lambda e: e[4]),
      ('context_version_name', 'STRING', lambda e: e[5]),
      ('context_ip_address', 'STRING', lambda e: e[6]),
      ('context_dma', 'STRING', lambda e: e[7]),
      ('context_device_type', 'STRING', lambda e: e[8]),
      ('context_device_manufacturer', 'STRING', lambda e: e[9]),
      ('context_start_version', 'STRING', lambda e: e[10]),
      ('context_os_name', 'STRING', lambda e: e[11]),
      ('context_device_id', 'STRING', lambda e: e[12]),
      ('context_language', 'STRING', lambda e: e[13]),
      ('context_device_model', 'STRING', lambda e: e[14]),
      ('context_country', 'STRING', lambda e: e[15]),
      ('context_region', 'STRING', lambda e: e[16]),
      ('event', 'INTEGER', lambda e: e[17]),
      ('event_text', 'STRING', lambda e: e[18]),
      ('id', 'STRING', lambda e: e[19]),
      ('received_at', 'STRING', lambda e: e[20]),
      ('sent_at', 'STRING', lambda e: e[21]),
      ('user_id', 'STRING', lambda e: e[22]),
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

  parser.add_argument('--input',
                      dest='input',
                      help='Path to the data file(s)')
  parser.add_argument('--dataset',
                      dest='dataset',
                      required=True,
                      help='BigQuery Dataset to write tables to. '
                           'Must already exist.')
  parser.add_argument('--table_name',
                      dest='table_name',
                      default='Event',
                      help='The BigQuery table name. Should not already exist.')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_options = PipelineOptions(pipeline_args)
  with beam.Pipeline(options=pipeline_options) as p:

    (p  # pylint: disable=expression-not-assigned
     | ReadFromText(known_args.input) # Read events from a file and parse them.
     | Event_Logging()
     | WriteToBigQuery(
         known_args.table_name, known_args.dataset, configure_bigquery_write()))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()