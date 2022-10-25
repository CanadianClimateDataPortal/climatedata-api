# Developer guidelines

## Running the API Locally

To run the API locally, you first need to make sure the `bf_scratch` directory 
(internal to our infrastructure) is mounted to the `./datasets` folder. 

Once this is done, you can run the API with the `python3 wsgi.py` command.

If you are testing on a remote machine, you will probably need to add the `--host 0.0.0.0` 
option so the API is accessible.

## Code Quality

A rudimentary `Makefile` is part of this project as to simplify and automate code
quality tools.

To use them, you must first install them in your development environment; 
`pip install -r requirements_dev.txt`

### Linting

To check the linting (Pylint), you can execute the command `make check-linting`.

### Formatting

For formatting, you can preview the changes beforehand with the 
`make check-formatting` command.

Changes can be applied using the `make fix-formatting` command.

Alternatively, you can use the tools directly:

```
# To check
$ autopep8 --max-line-length 120 --diff -v climatedata_api/charts.py climatedata_api/download.py [...] 
$ isort -c -v climatedata_api/ tests/

# To fix
$ autopep8 --max-line-length 120 --in-place -v climatedata_api/charts.py climatedata_api/download.py [...] 
$ isort -v climatedata_api/ tests/
```

### Testing

Testing for now is limited to API calls on a running local instance.

With a running API, (see **Running the API Locally** above), execute the
`make test` command.

Alternatively, you can also run the tests using the `pytest tests` command.

To run a specific test among the parametrized tests, you can run use the `-k` option:
`pytest tests/ -k {test_id}`
