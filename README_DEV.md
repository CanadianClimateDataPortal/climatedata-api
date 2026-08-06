# Developer guidelines

## Project setup with mise and uv

This project pins Python 3.9.16 in `.python-version` and `.mise.toml`. From the
repository root, install mise's tools (if needed), create the project environment,
and install the locked dependencies with:

```sh
mise install
uv sync --group dev
```

With mise shell activation enabled, `.venv` is automatically put on `PATH`
when entering the project directory. To use mise without changing your shell,
run commands through `mise exec --` instead:

```sh
mise --cd . run setup
mise --cd . run info
mise --cd . exec -- make test
```

`mise run info` prints the local reminders (dataset mount, Chrome, and update
commands). Running `mise run` with no task (or `mise tasks`) lists the available
tasks and displays the reminders. Use `mise run update` after intentionally
changing dependency constraints; it refreshes `uv.lock` and syncs `.venv`.

The existing `Makefile` is intentionally unchanged. Once mise is activated,
its commands continue to work normally (`make test`, `make check-lint`, and
so on). If you are not using mise, the equivalent setup is `uv venv --python
3.9.16` followed by `uv sync --group dev`.

## Running the API Locally

To run the API locally, you first need to make sure the `bf_scratch` directory
(internal to our infrastructure) is mounted to the `./datasets` folder.

Start the API over HTTP with:

```sh
mise run run
```

For local HTTPS, generate a self-signed certificate and start the HTTPS server:

```sh
mise run https
```

The HTTPS server listens at <https://127.0.0.1:5443/>. Your browser will warn
that the certificate is self-signed; that warning is expected for local use.
The generated key and certificate live under `.certs/` and are ignored by git.

If you are testing on a remote machine, use `uv run flask --app wsgi run --host 0.0.0.0`
(or configure `wsgi.py` to pass the host option) so the API is accessible.

The landing page at <http://127.0.0.1:5000/> is deliberately lightweight and is
useful for checking that the process is reachable.

### Browser access and CORS

The API allows browser requests from these HTTPS origins:

- `https://climatedata.ca`
- `https://donneesclimatiques.ca`
- `https://dev-en.climatedata.ca`
- `https://dev-fr.climatedata.ca`

CORS is configured in Flask, so it works consistently under mise, Docker, and
uWSGI. The nginx deployment configuration must not add a second
`Access-Control-Allow-Origin` header. Credentials are not enabled; the frontend
should use ordinary cross-origin requests without cookies.

### Selenium smoke check

With the API running in one terminal, open it in Chrome from another terminal:

```sh
mise run selenium
```

To check the local HTTPS server instead:

```sh
uv run python tools/selenium_smoke.py --url https://127.0.0.1:5443/
```

The Selenium check accepts the local self-signed certificate. The script uses
Selenium Manager, so a manually installed `chromedriver` is not required. Use `--headless` for CI or machines without a display. Chrome itself
must still be installed.

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
