# Maintenance PR disposition — 2026-11-12

The owner requested resolution/closure of the open PRs and current v0.9 documentation. No branch protection, required check or immutable released artifact was changed.

| PR | Disposition |
| --- | --- |
| [#458](https://github.com/araditc/Kafdeck/pull/458) | Merged as `6af01b5` after its original approval and green checks; the separately locked historical publisher/source identity remains unchanged. |
| [#447](https://github.com/araditc/Kafdeck/pull/447), [#448](https://github.com/araditc/Kafdeck/pull/448), [#449](https://github.com/araditc/Kafdeck/pull/449), [#450](https://github.com/araditc/Kafdeck/pull/450), [#452](https://github.com/araditc/Kafdeck/pull/452) | Superseded by the consolidated #451 candidate; closed rather than individually merging stale or mixed-version updates. |
| [#451](https://github.com/araditc/Kafdeck/pull/451) | CodeQL mismatch fixed by pinning init/autobuild/analyze together, and all six proposed updates tested against released main at `4a572e61fa6b93b6c77a8bcd82add7726822a9ee`. The candidate was not admitted: W77 evidence and both OCI startup jobs exited unsuccessfully at their image-scan steps. Closed/deferred; retained current dependencies and the complete candidate for a follow-up with successful scan evidence. |

The failed scan steps are in [43249481705](https://github.com/araditc/Kafdeck/actions/runs/43249481705) and [43249481939](https://github.com/araditc/Kafdeck/actions/runs/43249481939). Their failure alone is not a claim that a specific new CVE exists in the already-published v0.9 image. No scan gate was suppressed and no successful product/load test was manually repeated. Re-admitting the deferred maintenance candidate requires normal fresh successful checks.

The dependency policy now groups `github/codeql-action/*`, as required by CodeQL's same-version contract. This prevents the observed split init/autobuild/analyze update failure without changing the currently admitted action version.
