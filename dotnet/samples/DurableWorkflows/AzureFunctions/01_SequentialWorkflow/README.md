# Sequential Workflow Sample

This sample demonstrates how to use the Microsoft Agent Framework to create an Azure Functions app that hosts durable workflows with sequential executor chains. It showcases two workflows that share a common executor, demonstrating executor reuse across workflows.

## Key Concepts Demonstrated

- Defining workflows with sequential executor chains using `WorkflowBuilder`
- Sharing executors across multiple workflows (the `OrderLookup` executor is used by both workflows)
- Registering workflows with the Function app using `ConfigureDurableWorkflows`
- Durable orchestration ensuring workflows survive process restarts and failures
- Starting workflows via HTTP requests
- Invoking a workflow from your own function code with `DurableTaskClient.AsWorkflowClient`, without an `HttpClient`
- Viewing workflow execution history and status in the Durable Task Scheduler (DTS) dashboard

## Workflows

This sample defines two workflows:

1. **CancelOrder**: `OrderLookup` → `OrderCancel` → `SendEmail` — Looks up an order, cancels it, and sends a confirmation email.
2. **OrderStatus**: `OrderLookup` → `StatusReport` — Looks up an order and generates a status report.

Both workflows share the `OrderLookup` executor, which is registered only once by the framework.

## Environment Setup

See the [README.md](../../README.md) file in the parent directory for more information on how to configure the environment, including how to install and run common sample dependencies.

## Running the Sample

With the environment setup and function app running, you can test the sample by sending HTTP requests to the workflow endpoints.

You can use the `demo.http` file to trigger the workflows, or a command line tool like `curl` as shown below:

### Cancel an Order

Bash (Linux/macOS/WSL):

```bash
curl -X POST http://localhost:7071/api/workflows/CancelOrder/run \
    -H "Content-Type: text/plain" \
    -d "12345"
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post `
    -Uri http://localhost:7071/api/workflows/CancelOrder/run `
    -ContentType text/plain `
    -Body "12345"
```

The response will confirm the workflow orchestration has started:

```json
{
    "runId": "abc123def456",
    "message": "Workflow orchestration started for CancelOrder."
}
```

Workflow run responses use JSON by default. Include `Accept: text/plain` to request the legacy plain-text representation instead.

> **Tip:** You can provide a custom run ID by appending a `runId` query parameter:
>
> ```bash
> curl -X POST "http://localhost:7071/api/workflows/CancelOrder/run?runId=my-order-123" \
>     -H "Content-Type: text/plain" \
>     -d "12345"
> ```
>
> If not provided, a unique run ID is auto-generated.

### Wait for the Workflow Result

By default, the HTTP endpoint returns `202 Accepted` immediately with the run ID. To wait for the workflow to complete, set `waitForResponse=true` in the query string. The endpoint waits for up to 10 seconds by default; set `timeoutSeconds` to use a timeout from 1 to 200 seconds. If the workflow is still running when the timeout expires, the endpoint returns the same `202 Accepted` response as the default asynchronous invocation.

Bash (Linux/macOS/WSL):

```bash
curl -X POST "http://localhost:7071/api/workflows/CancelOrder/run?waitForResponse=true&timeoutSeconds=30" \
    -H "Content-Type: text/plain" \
    -d "12345"
```

PowerShell:

```powershell
Invoke-RestMethod -Method Post `
    -Uri "http://localhost:7071/api/workflows/CancelOrder/run?waitForResponse=true&timeoutSeconds=30" `
    -ContentType text/plain `
    -Body "12345"
```

The response is JSON by default:

```json
{
    "runId": "abc123def456",
    "workflowStatus": "Completed",
    "result": "Cancellation email sent for order 12345 to jerry@example.com."
}
```

To get only the workflow result as plain text, include the `Accept: text/plain` header:

```bash
curl -X POST "http://localhost:7071/api/workflows/CancelOrder/run?waitForResponse=true" \
    -H "Content-Type: text/plain" \
    -H "Accept: text/plain" \
    -d "12345"
```

```text
Cancellation email sent for order 12345 to jerry@example.com.
```

The `x-ms-wait-for-response` header remains supported for backward compatibility. A wait timeout returns the same `202 Accepted` response as the default asynchronous invocation. Client request cancellation instead aborts the HTTP wait without returning a response, but the durable workflow continues; callers that need to recover should supply `runId` up front and query its status later.

In the function app logs, you will see the sequential execution of each executor:

```text
│ [Activity] OrderLookup: Starting lookup for order '12345'
│ [Activity] OrderLookup: Found order '12345' for customer 'Jerry'
│ [Activity] OrderCancel: Starting cancellation for order '12345'
│ [Activity] OrderCancel: ✓ Order '12345' has been cancelled
│ [Activity] SendEmail: Sending email to 'jerry@example.com'...
│ [Activity] SendEmail: ✓ Email sent successfully!
```

### Get Order Status

```bash
curl -X POST http://localhost:7071/api/workflows/OrderStatus/run \
    -H "Content-Type: text/plain" \
    -d "12345"
```

The `OrderStatus` workflow reuses the same `OrderLookup` executor and then generates a status report:

```text
│ [Activity] OrderLookup: Starting lookup for order '12345'
│ [Activity] OrderLookup: Found order '12345' for customer 'Jerry'
│ [Activity] StatusReport: Generating report for order '12345'
│ [Activity] StatusReport: ✓ Order 12345 for Jerry: Status=Active, Date=2025-01-01
```

### Invoking a Workflow from Function Code

The endpoints above are generated by the framework. `OrderFunctions.cs` shows the other direction: hand-written functions that start the same `CancelOrder` workflow from your own code, without creating an `HttpClient` or knowing the workflow's HTTP route. This alternative approach is useful when you want to start a workflow from a queue trigger, timer trigger, etc. or if you need more control over the HTTP request/response than the generated endpoints provide.

```csharp
// Get a workflow client from the [DurableClient] binding, then start the workflow by name.
IWorkflowClient workflows = durableClient.AsWorkflowClient(context);
IWorkflowRun run = await workflows.RunAsync("CancelOrder", orderId);
```

The workflow is started through the durable backend rather than through the workflow's generated HTTP route, so the call path does not depend on that route and the same two lines work from any trigger type — queue, timer, Event Grid, Service Bus. If the workflow name is not registered, `RunAsync` throws `WorkflowNotRegisteredException` immediately instead of scheduling an orchestration that no worker can execute.

Start the workflow and get back a run ID:

```bash
curl -X POST http://localhost:7071/api/orders/12345/cancel
```

```text
Workflow orchestration started for CancelOrder. Orchestration runId: abc123def456
```

Durable workflow runs also implement `IAwaitableWorkflowRun`, so the same handle can be awaited for the final result:

```bash
curl -X POST http://localhost:7071/api/orders/12345/cancel-and-wait
```

```text
Cancellation email sent for order 12345 to jerry@example.com.
```

### Viewing Workflows in the DTS Dashboard

After running a workflow, you can navigate to the Durable Task Scheduler (DTS) dashboard to visualize the completed orchestration, inspect inputs/outputs for each step, and view execution history.

If you are using the DTS emulator, the dashboard is available at `http://localhost:8082`.
