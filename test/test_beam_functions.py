from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from datetime import datetime
from src.helpers.beam_functions import filter_transaction_amount, filter_timestamp, map_date_and_transaction_amount

import unittest
import apache_beam as beam


class TestBeamFunctions(unittest.TestCase):

    def test_transaction_amount_filter(self):

        data = [
            ["2005-01-01 00:00:00 UTC","30"],
            ["2015-01-01 00:00:00 UTC","10"],
            ["2015-01-01 00:00:00 UTC","30"]
        ]

        with TestPipeline() as p:

            input = p | beam.Create(data)

            output = input | beam.Filter(filter_transaction_amount, 1, 20)

            assert_that(
                output,
                equal_to([['2005-01-01 00:00:00 UTC', '30'],
                          ['2015-01-01 00:00:00 UTC', '30']])
            )

    def test_timestamp_filter(self):

        data = [
            ["2005-01-01 00:00:00 UTC","30"],
            ["2015-01-01 00:00:00 UTC","10"],
            ["2015-01-01 00:00:00 UTC","30"]
        ]

        with TestPipeline() as p:

            input = p | beam.Create(data)

            output = input | beam.Filter(filter_timestamp, 0, datetime(year=2010,
                                                                       month=1,
                                                                       day=1))

            assert_that(
                output,
                equal_to([["2015-01-01 00:00:00 UTC","10"],
                          ["2015-01-01 00:00:00 UTC","30"]])
            )