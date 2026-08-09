# Security Policy

## Reporting a vulnerability

Do not open a public issue for a suspected vulnerability or exposed credential. Contact the repository owner privately through the verified contact method on the GitHub profile and include the affected component, reproduction steps, impact, and any proposed mitigation. Do not access data belonging to another project or perform destructive testing.

## Supported version

The deployed `main` branch is the supported challenge release. Security fixes are applied there and redeployed to Zerops after automated and live verification.

## Operational expectations

- Rotate any credential pasted into chat, logs, screenshots, or an issue.
- Store dashboard, admin, project, database, Valkey, Qdrant, SMTP, and provider credentials only as Zerops secrets.
- Keep PostgreSQL, Valkey, Qdrant, and worker services private.
- Configure the same generic-webhook hostname allowlist on API and worker.
- Treat notification delivery as at least once and make provider receivers idempotent.
- Review retention, legal hold, and archive policy before ingesting regulated production data.
