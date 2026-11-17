# Schema compatibility planning: CodeQL alert 3 review

Reviewed on 2026-11-17 against candidate `1881aaf5a49f1ab0fc3d3ec99a27ade0882f9e4c`. Alert [#3](https://github.com/araditc/Kafdeck/security/code-scanning/3), rule `cs/user-controlled-bypass`, identifies the enum validation in `SchemaMutationPlanner.PlanCompatibilityAsync` and the conditional call to `SchemaMutationCanonicalization.AuthorizationResource`.

## Disposition: false positive

`AuthorizationResource(subject)` in `SchemaMutationContracts.cs` only formats `schema/{subject}`. It performs no authorization, credential validation, provider call or write. Global scope uses the other resource string, `schema-compatibility/global`. Selecting one formatter does not skip a security check.

Both HTTP preview routes in `KafdeckSchemaMutationEndpoints.cs` execute `SchemaAlter` preflight before planning. The subject route supplies the route subject and server-fixed `Subject` scope; the global route supplies server-fixed `Global` scope and no subject. The body contract contains only `RequestedMode`. A body cannot choose a different scope or authorization resource.

The planner rejects undefined scope/mode, `Unknown` mode and a subject supplied with global scope before any provider capability reads. Every successful compatibility plan includes the explicit `SchemaAlter` authorization target, cluster and subject/global resource. Risk classification remains at least High, including the valid `None` compatibility mode. Planning is not execution authorization: shared mutation admission, approval, policy and execution controls remain unchanged.

## New focused regression evidence

Nine cases in `V05SchemaMutationTests`, filter `FullyQualifiedName~Codeql3`, passed with zero failures/skips:

- Two undefined scopes and two invalid/unknown modes return `InvalidInput`, no plan or execution material and zero provider capability calls.
- Subject and global scopes, each with `Full` and `None` modes, retain their correct `SchemaAlter` target, cluster and minimum High risk.
- Global scope with a subject is rejected before provider reads.

These are planner tests with controlled catalog/provider observations. They do not claim a fresh Schema Registry deployment or new end-to-end authorization campaign. The HTTP preflight and execution boundaries were inspected in unchanged candidate source; admitted schema/shared-kernel regressions retain their original identity.

No runtime guard, scanner rule, severity, approval or audit behavior is changed. The additive tests protect the demonstrated distinction between resource naming and authorization. [V1-5 acceptance](../releases/v1.0-security-support-admission.md) records the source-bound scan and support evidence.
