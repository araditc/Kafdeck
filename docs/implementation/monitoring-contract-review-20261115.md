# Monitoring preview contract review — 2026-11-15

The metadata-only export from the monitoring source tree was reviewed before admitting the API/operator snapshots. Stable and Experimental operation selection and existing compiled route registrations are unchanged. Existing canonical OpenAPI documents are unchanged. The one request-schema delta appends `monitoringRead` / numeric 73 to the action enum in `FamilyServiceRequestTarget`; values 1–72, required fields and other schemas retain their identity.

Operator keys add `Kafdeck:Monitoring`, `Kafdeck:Monitoring:IngestOnlyPort` and `Kafdeck:Monitoring:MetadataClusters`; no old keys are removed. New receiver-relative accesses belong to monitoring source/rule storage and sampling; Program adds the metadata-cluster lookup. Producer fingerprint updates cover the new monitoring classes and the reviewed Program, registered-runtime and initial-owner authorization integration. CLI/MCP baselines remain unchanged.

Monitoring browser/ingestion endpoints remain Experimental. This admission does not grant a new Stable monitoring contract or change the v1.0 owner policy, provider versions or published v0.9 evidence. Metadata fixtures are not runtime authorization or installation proof; prior focused monitoring/Windows acceptance is reused.

The integrated CI run passed 2524 backend tests with two expected drift failures and 81 environment-specific skips. The exported differences were inspected, then only the API/operator snapshots were admitted. Four focused baseline comparisons are rerun after admission; a full runtime campaign is not manually repeated.
