---
name: pull-request-workflow
description: How to open a pull request in this repo and shepherd it through automated review and CI. Use this whenever the user asks to create, open, or draft a PR, push changes for review, respond to Copilot review comments, or check on failing CI / integration tests for a PR in agent-framework-durable-extension.
---

# Pull request workflow

This repo has two quirks that trip agents up: GitHub auth resolves to the wrong
account by default, and the automated review/CI loop has a specific rhythm.

## 1. Use the right GitHub account

Microsoft employees typically have two accounts: an Enterprise Managed User
(e.g. `alice_microsoft`) and a personal one (e.g. `alice`). Often only the
personal account can write to this repo; the EMU account gets
`403 Unauthorized: As an Enterprise Managed User, you cannot access this content`.

For this repo, you'll need to use the personal account. You can check which account
is active with `gh auth status`. If it's the wrong one, switch with `gh auth login`.
If there is a `GH_TOKEN` environment variable set, it will override the active account
and you'll need to unset it prior to each command that needs the personal account.

```powershell
$env:GH_TOKEN = (gh auth token -u <personal-account>)
gh pr create ...
```

`git push` may need more than `GH_TOKEN`. The Windows credential manager is first in
the helper chain and may silently supply the *other* account's credentials.
Clear the chain for the push:

```powershell
$env:GH_TOKEN = (gh auth token -u <personal-account>)
git -c credential.helper= -c credential.https://github.com.helper='!gh auth git-credential' push
```

Verify with `gh api user --jq .login` before doing anything that writes.

## 2. Write the PR description

Open PRs as drafts unless the user says otherwise. Structure the body around
these four questions:

1. **Why** — what problem or upstream change motivates this. Link the issue.
2. **What's changing** — the shape of the change at a conceptual level.
3. **Compatibility** — split explicitly into what still works and what breaks,
   with short before/after snippets for anything a consumer must react to.
4. **What reviewers should look at** — the judgment calls, not the mechanics.
   Name the decisions you'd want challenged.

Do **not** enumerate individual code changes. The diff already says what changed
line by line; the description exists to convey intent and the reasoning a
reviewer can't recover from reading the diff. A reviewer should be able to read
it and know where to spend their attention.

## 3. Trigger and monitor Copilot review

If Copilot doesn't auto-review the draft PR, request it explicitly. The GraphQL
path (`gh pr edit --add-reviewer Copilot`) fails with "Could not resolve user";
use REST:

```powershell
gh api -X POST repos/<owner>/<repo>/pulls/<n>/requested_reviewers -f 'reviewers[]=Copilot'
```

Reviews land in roughly 1–5 minutes. Poll for them, filtering by author — both so
you can tell a fresh round from your own replies, and because the guidance below
applies only to Copilot:

```powershell
gh api repos/<owner>/<repo>/pulls/<n>/reviews --jq '[.[]|select(.user.login=="Copilot")]|length'
gh api "repos/<owner>/<repo>/pulls/<n>/comments?per_page=100" --jq '.[]|select(.user.login=="Copilot")|"\(.id) \(.path):\(.line)\n\(.body)\n---"'
```

Evaluate each comment on its merits — some are genuinely wrong or not worth the
churn, and saying so with a reason is a fine outcome. Fix the ones that are
right, then reply to each thread so the reasoning is on the record:

```powershell
gh api -X POST repos/<owner>/<repo>/pulls/<n>/comments/<comment-id>/replies -f "body=..."
```

The reply route needs the PR number; `pulls/comments/<id>/replies` returns 404.

If pushing fixes does not automatically re-trigger review, request Copilot again,
and keep looping until a round comes back with no new comments.

A clean round is not on its own a signal to stop. Every fix commit triggers
another review, so a "no new comments" result can still be followed by a round
that finds something. Confirm no further review arrives after your last push
before calling it done.

Update the PR description to reflect any changes in intent or compatibility as you go.

### Human review comments are different — do not auto-respond

Everything above applies to Copilot only. When a **human** leaves review
comments, do not reply on the user's behalf and do not push fixes for them
unless the user asks you to.

Two reasons this matters. Human reviewers encode context the diff doesn't carry
— team priorities, the history behind a design, work already planned elsewhere —
so the right response often isn't derivable from the code alone. And a reply
posted through the user's account reads as the user speaking; committing them to
a position they haven't seen is worse than a slow reply.

Instead, summarize what the reviewer said, note which points look straightforward
versus which need the user's judgment, and let them decide. If they hand you
specific comments, act on those and leave the rest alone.

## 4. Monitor CI

Every commit triggers GitHub Actions (build, unit tests, format, Python matrix)
plus Azure DevOps integration tests. The Actions checks settle in 1–4 minutes;
the integration tests take around 15.

Because of that gap, **work Copilot's feedback first** — it arrives long before
the integration tests finish, and fixing it produces another commit anyway, which
restarts the integration run. Waiting on integration tests before addressing
review comments wastes a full cycle.

```powershell
gh pr checks <n> --repo <owner>/<repo>
gh run view --repo <owner>/<repo> --job <job-id> --log-failed
```

Poll in a loop rather than one-shot checking, and surface failures as soon as
they appear instead of at the end.

### Failures worth anticipating

`check-format` enforces `dotnet/.editorconfig`, which requires **utf-8-bom** and
**LF** line endings for `.cs` files. Newly created files are the usual offender,
and the failure (`error CHARSET` / `error ENDOFLINE`) only surfaces in CI if you
don't check locally. Verify before pushing:

```powershell
dotnet format <project.csproj> --verify-no-changes
```

This repo's .NET tests run on Microsoft.Testing.Platform (set in `global.json`),
which is what CI uses via `dotnet test`. Pass the target framework explicitly:

```powershell
dotnet test <test.csproj> -f net10.0
```

If you pass a framework the project doesn't target, the failure is reported as
`global.json defines test runner to be Microsoft.Testing.Platform. All projects
are using VSTest test runner` — which points at the runner rather than the real
problem. Check the project's `TargetFrameworks` before assuming the runner is
misconfigured.
