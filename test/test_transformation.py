from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import unittest
import apache_beam as beam
from src.helpers.beam_functions import aggregate_transaction_amount_by_date


class TestTransformation(unittest.TestCase):

    def test_transformation(self):

        data = [
            ["2005-01-01 00:00:00 UTC","30"],
            ["2015-01-01 00:00:00 UTC","10"],
            ["2015-01-01 00:00:00 UTC","30"],
            ["2015-01-01 00:00:00 UTC","21"]
        ]

        with TestPipeline() as p:

            input = p | beam.Create(data)

            output = input | aggregate_transaction_amount_by_date(1, 0)

            assert_that(
                output,
                equal_to([{"date": "2015-01-01", "total_amount": 51.0}])
            )


if __name__ == '__main__':
    unittest.main()
