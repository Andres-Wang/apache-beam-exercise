Dependencies are managed by the `requirements.txt` file at the moment.\
Run below command for the unit testings, followed by the entry point of the program.\
```python -m pytest && python -u src/main.py```

This program processes the file `gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv`, and writes the output in `output/` folder as a suppressed file. \
All the map and filter functions are saved in `src/helpers/beam_functions`, including a composite transformation function that wraps all the logic together. \
All beam functions are covered by unit tests.
