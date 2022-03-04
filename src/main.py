import json
import logging
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText, ReadFromText
from src.helpers.df_utils import get_index_list
from src.helpers.beam_functions import aggregate_transaction_amount_by_date
from src.helpers.file_sys_utils import remove_dir_if_exists


class DataPipelineJob(object):

    def __init__(self, config_path='src/config.json'):
        self.config_path = config_path
        self.config = None
        self.pipeline_options = None

    def load_config(self, config_path):
        with open(config_path, 'r') as f:
            self.config = json.load(f)

    def load_pipeline_options(self):
        self.pipeline_options = PipelineOptions(
            runner='DirectRunner'
        )

    def run(self):
        self.load_config(self.config_path)
        self.load_pipeline_options()

        remove_dir_if_exists(self.config["output_folder"])

        with beam.Pipeline(options=self.pipeline_options) as pipeline:
            transactions = (pipeline
                            | ReadFromText(self.config["input_path"], skip_header_lines=True)
                            | beam.Map(lambda x: x.split(","))
                            )

            index_list = get_index_list(self.config["input_path"])
            transaction_amount_index, timestamp_index = index_list.index("transaction_amount"), index_list.index("timestamp")

            result = transactions | aggregate_transaction_amount_by_date(transaction_amount_index, timestamp_index)

            result | WriteToText(file_path_prefix=self.config["output_path"], shard_name_template='', num_shards=1,
                                 compression_type="gzip", file_name_suffix=".jsonl.gz")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    data_pipeline_job = DataPipelineJob()
    data_pipeline_job.run()
