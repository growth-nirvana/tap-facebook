# tap-facebook

`tap-facebook` is a Singer tap for facebook.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation


```bash
pipx install git+https://github.com/MeltanoLabs/tap-facebook-sdk.git
```

## Configuration

### Accepted Config Options

This tap requires the following environmental variables to be set in ```.env```

- [ ] `TAP_FACEBOOK_ACCOUNT_ID` facebook account id
- [ ] `TAP_FACEBOOK_ACCESS_TOKEN` facebook access token


### Meltano Variables

The following config values need to be set in order to use with Meltano. These can be set in `meltano.yml`, via
```meltano config tap-facebook set --interactive```, or via the env var mappings shown above.

- [ ] `access_token:` access token from TAP_FACEBOOK_ACCESS_TOKEN variable
- [ ] `start_date:` start date
- [ ] `end_date:` end_date 
- [ ] `account_id:` account id from TAP_FACEBOOK_ACCOUNT_ID variable
- [ ] `api_version:` api version

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-facebook --about
```

### Authentication

We have BearerTokenAuthenticator in client.py for authentication

## Usage

### API Limitation - Rate Limits

Unless your

You can easily run `tap-facebook` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-facebook --version
tap-facebook --help
tap-facebook --config CONFIG --discover > ./catalog.json
```

## Contributing

This project uses parent-child streams. Learn more about them [here](https://gitlab.com/meltano/sdk/-/blob/main/docs/parent_streams.md).

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_facebook/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-facebook` CLI interface directly using `poetry run`:

```bash
poetry run tap-facebook --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-facebook
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-facebook --version
# OR run a test `elt` pipeline:
meltano elt tap-facebook target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
