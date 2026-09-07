# Security Policy

## Supported Versions

NimbusKV is currently pre-1.0 and moves quickly. Security fixes are
only guaranteed for the latest released minor version.

| Version | Supported |
| ------- | --------- |
| 3.0.x   | ✅ |
| < 3.0   | ❌ |

Once the project reaches a stable 1.0, this table will be expanded to
cover a longer support window (e.g. current + previous minor).

## Reporting a Vulnerability

**Please do not open a public GitHub issue for security
vulnerabilities.** Discussing a vulnerability in the open before a
fix is available puts every user of the library at risk.

Instead, report it privately using one of these two channels:

1. **Preferred:** use GitHub's private vulnerability reporting —
   go to the [Security tab](../../security/advisories/new) of this
   repository and click "Report a vulnerability." This opens a
   private advisory visible only to the maintainer until a fix is
   ready.
2. **Alternative:** email **hemanshuvaidya64@gmail.com** directly
   with a description of the issue, steps to reproduce, and the
   affected version(s).

### What to include

To help triage quickly, please include:

* The version of `nimbuskv` affected (and backend in use, if
  relevant — memory / SQLite / Redis)
* A minimal reproduction, if you have one
* The potential impact as you understand it (e.g. data corruption
  under concurrent access, unsafe deserialization, denial of
  service)

### What to expect

* **Acknowledgment:** within 5 business days.
* **Status updates:** at least every 2 weeks while the issue is
  worked on.
* **Disclosure:** once a fix is released, the reporter will be
  credited (unless anonymity is requested) in the release notes and
  `CHANGELOG.md`, coordinated with the reporter on timing.

### Scope note specific to this project

NimbusKV's persistence layer uses `pickle` for the local SQLite
backend by design (trusted local file). If you're evaluating a
deployment where the backend or its contents could be
untrusted/network-reachable, that's a real risk worth reporting even
if it's "expected behavior" today — see the open design discussion
around the planned Warehouse (v4.0) component, where wire-format
serialization is explicitly being moved away from `pickle` for this
exact reason.

Thank you for helping keep NimbusKV and its users safe.
