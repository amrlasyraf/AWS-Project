# GitHub Sync Setup (Option B) for Kestra

To enable "Option B" (Git-driven development), follow these steps to link your `AWS-Project` repository with your EC2 Kestra instance.

## 🔑 1. Create a GitHub Personal Access Token (PAT)
A **Classic PAT** is recommended for full compatibility:

1.  Log in to GitHub and click your profile picture > **Settings**.
2.  On the left sidebar, click **Developer settings** (at the bottom).
3.  Click **Personal access tokens** > **Tokens (classic)**.
4.  Click **Generate new token** > **Generate new token (classic)**.
5.  **Note:** Give it a name like `Kestra_EC2_Sync`.
6.  **Expiration:** Select "No expiration" or a duration that suits your security policy.
7.  **Select Scopes:**
    *   [x] `repo` (Full control of private repositories)
8.  Click **Generate token**.
9.  **IMPORTANT:** Copy the token immediately. You won't be able to see it again!

---

## 🛡️ 2. Add as a Kestra Secret
**Do not hardcode your token.** Instead, use Kestra's built-in Secret management:
1.  Open your Kestra UI (on EC2).
2.  Navigate to **Settings** > **Secrets** (or via environment variables).
3.  Create a secret named `GITHUB_TOKEN`.
4.  Paste your GitHub PAT as the value.

---

## 🔄 3. Kestra Registry Flow
The flow has been drafted in `kestra/_system/github_sync.yaml`. It uses the `SyncNamespaceFiles` task to pull your code into Kestra's internal storage (`Namespace Files`).

```yaml
id: github_sync
namespace: amrlasyraf.system

tasks:
  - id: sync_repository
    type: io.kestra.plugin.git.SyncNamespaceFiles # Syncs repo content to Namespace Files
    url: https://github.com/amrlasyraf/AWS-Project.git
    branch: main
    username: amrlasyraf
    password: "{{ secret('GITHUB_TOKEN') }}"
    namespace: amrlasyraf # Target Kestra namespace
    dryRun: false

triggers:
  - id: hourly_sync
    type: io.kestra.plugin.core.trigger.Schedule
    cron: "0 * * * *"
```
