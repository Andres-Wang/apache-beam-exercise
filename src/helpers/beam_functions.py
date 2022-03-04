from datetime import datetime
import apache_beam as beam


def filter_transaction_amount(transaction, transaction_amount_index, less_than):
    """
    Filters out all transactions that has an amount lower than "less_than" param.
    :param transaction_amount_index: The index of the transaction amount column
    :param less_than: All transactions lower than this amount will be filtered out
    :return: bool
    """
    transaction_amount = float(transaction[transaction_amount_index])
    return transaction_amount > less_than


def filter_timestamp(transaction, timestamp_index, before):
    """
    Filters out all transactions that are earlier than given timestamp.
    :param timestamp_index: The index of the timestamp column
    :param before: All transactions earlier than this timestamp will be filtered out
    :return: bool
    """
    timestamp = datetime.strptime(transaction[timestamp_index], "%Y-%m-%d %H:%M:%S UTC")
    return timestamp > before


def map_date_and_transaction_amount(transaction, timestamp_index, transaction_amount_index):
    """
    Maps the transaction into a tuple of date and transaction_amount
    :param timestamp_index: The index of the timestamp column
    :param transaction_amount_index: The index of the transaction amount column
    :return: tuple
    """
    return transaction[timestamp_index].split()[0], float(transaction[transaction_amount_index])


@beam.ptransform_fn
def aggregate_transaction_amount_by_date(pcoll, transaction_amount_index, timestamp_index):
    return (pcoll
            | "Filter by transaction_amount" >>
            beam.Filter(filter_transaction_amount, transaction_amount_index, 20)
            | "Filter by timestamp" >>
            beam.Filter(filter_timestamp, timestamp_index, datetime(year=2010,
                                                                    month=1,
                                                                    day=1))
            | "Map transaction to date and transaction_amount" >>
            beam.Map(map_date_and_transaction_amount, timestamp_index, transaction_amount_index)
            | "Aggregate sum by date field" >>
            beam.CombinePerKey(sum)
            | "Map to jsonl format" >>
            beam.Map(lambda x: {"date": x[0], "total_amount": x[1]})
            )
