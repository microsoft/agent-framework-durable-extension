# Sample Integration Tests

Integration tests that validate the Durable Agent Framework samples by running them as Azure Functions.

## Setup

### 1. Create `.env` file

Copy `.env.example` to `.env` and fill in your Azure credentials:

```bash
cp .env.example .env
```

Required variables:
- `AzureWebJobsStorage`
- `FUNCTIONS_WORKER_RUNTIME`

Agent samples also require `FOUNDRY_PROJECT_ENDPOINT` and `FOUNDRY_MODEL`.
The `13_subworkflow_hitl` sample hosts no agents and needs no model configuration.

Run `az login` before the tests. The hosted samples use `AzureCliCredential`.

For GitHub Actions, configure the same model endpoints plus `AZURE_CLIENT_ID`,
`AZURE_TENANT_ID`, and `AZURE_SUBSCRIPTION_ID` as secrets on the `integration`
environment. The workflow uses Azure Login with OpenID Connect so
`AzureCliCredential` can authenticate without a stored client secret.

### 2. Start required services

Install Azure Functions Core Tools 4 and start Azurite. These samples use the Azure Storage
backend and do not require the Durable Task Scheduler emulator or its connection string.

**Azurite (for orchestration tests):**
```bash
docker run -d -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite:3.37.0
```

Azurite 3.37.0 accepts the durable extension's storage API versions without disabling validation.
Samples use the GA extension bundle with version range `[4.38.1, 5.0.0)`.

## Running Tests

The tests automatically start and stop the Azure Functions app for each sample.

Run them from `python/packages/azurefunctions`. That package is a standalone uv project,
because azure-functions-durable 2.x requires Python 3.13, so it has its own environment.
Install its test dependencies first:

```bash
uv sync --group dev --group test
```

### Run all sample tests
```bash
uv run pytest tests/integration_tests -v
```

### Run specific sample
```bash
uv run pytest tests/integration_tests/test_01_single_agent.py -v
```

### Run with verbose output
```bash
uv run pytest tests/integration_tests -sv
```

## How It Works

Each test file uses pytest markers to automatically configure and start the function app:

```python
pytestmark = [
    pytest.mark.sample("01_single_agent"),
    pytest.mark.usefixtures("function_app_for_test"),
]
```

The `function_app_for_test` fixture:
1. Loads environment variables from `.env`
2. Validates required variables are present
3. Starts the function app on a dynamically allocated port
4. Waits for the app to be ready
5. Runs your tests
6. Tears down the function app

## Troubleshooting


**Missing environment variables:**
Ensure your `.env` file contains all required variables from `.env.example`.

**Tests timeout:**
Check that Azure OpenAI credentials are valid and the service is accessible.
