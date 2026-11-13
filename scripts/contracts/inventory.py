"""Read-only declared-contract index. Not a runtime route-completeness claim."""
import argparse, hashlib, json, pathlib, re, subprocess, textwrap

ROOT = pathlib.Path(__file__).resolve().parents[2]

def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def collect():
    files = subprocess.check_output(["git", "ls-files", "src", "providers/terraform", "deploy/helm/kafdeck", "docs/api"], cwd=ROOT, text=True).splitlines()
    sources = []
    routes = []
    catalogs = []
    for name in files:
        p = ROOT / name
        if p.suffix not in (".cs", ".go", ".json", ".yaml"):
            continue
        if name.startswith("src/frontend/"):
            continue
        sources.append({"path": name, "sha256": digest(p)})
        if name.startswith("src/backend/Kafdeck.Api/") and p.suffix == ".cs":
            text = p.read_text(encoding="utf-8-sig")
            for m in re.finditer(r'\b(\w+)\.Map(Get|Post|Put|Patch|Delete|Methods|Group)\(\s*(\$?)"((?:\\.|[^"\\])*)"', text):
                routes.append({"file": name, "line": text.count("\n", 0, m.start()) + 1,
                               "receiver": m[1], "registration": m[2], "template": m[4], "interpolated": bool(m[3])})
        if name.startswith("docs/api/openapi-") and p.suffix == ".json":
            d = json.loads(p.read_text(encoding="utf-8-sig"))
            operations = [{"method": method.upper(), "path": path, "operationId": op.get("operationId"),
                           "requestBody": op.get("requestBody"), "responses": op.get("responses", {})}
                          for path, methods in d.get("paths", {}).items() for method, op in methods.items()
                          if method in ("get", "post", "put", "patch", "delete", "head", "options")]
            catalogs.append({"path": name, "version": d.get("info", {}).get("version"),
                             "operationCount": len(operations), "schemaNames": sorted(d.get("components", {}).get("schemas", {})),
                             "operationIdentities": [{k: v for k, v in o.items() if k not in ("requestBody", "responses")} for o in operations],
                             "payloadAndErrorSchemasSha256": hashlib.sha256(json.dumps(operations, sort_keys=True).encode()).hexdigest()})
    cli = (ROOT / "src/cli/Kafdeck.Cli/CliApplication.cs").read_text(encoding="utf-8-sig")
    application = cli.split("public sealed record CliInvocation", 1)[0]
    exit_codes = {name: int(value) for name, value in re.findall(r'public const int (\w+) = (\d+);', application)}
    help_text = re.search(r'public const string Usage\s*=\s*"""(.*?)""";', cli, re.S)[1]
    terraform = []
    for name in files:
        if name.startswith("providers/terraform/internal/provider/") and name.endswith(".go") and not name.endswith("_test.go"):
            terraform.append({"file": name, "stateAndConfigurationFields": sorted(set(re.findall(r'tfsdk:"([^"]+)"', (ROOT / name).read_text())))})
    return {"sourceCommit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "productSrcTree": subprocess.check_output(["git", "rev-parse", "HEAD:src"], cwd=ROOT, text=True).strip(),
            "classificationStatus": "proposal; no new stability guarantee", "kind": "declared source inventory",
            "limits": ["Literal Map registrations only; dynamic builders, group prefixes and conditionally mapped routes require their existing runtime inventory tests.",
                       "Static JSON catalogs are versioned artifacts, not proof that every runtime endpoint is published there.",
                       "C# generated OpenAPI, CLI named reads, MCP generated tool schemas and configuration loader remain source-bound references; not omitted stable promises."],
            "jsonCatalogs": catalogs, "literalRegistrations": routes, "cliExitCodes": exit_codes,
            "cliDeclaredUsage": textwrap.dedent(help_text).strip(), "terraformDeclaredFields": terraform,
            "sourceFingerprints": sources}

def observe_compiled(directory, expected_usage):
    """Only local help and MCP discovery; never calls a backend tool."""
    cli = directory / "bin/Kafdeck.Cli/release/kafdeck.dll"
    mcp = directory / "bin/Kafdeck.Mcp/release/kafdeck-mcp.dll"
    help_run = subprocess.run(["dotnet", str(cli), "--help"], capture_output=True, text=True, encoding="utf-8", timeout=20, check=True)
    assert help_run.stdout.strip() == expected_usage, "Compiled CLI help does not match the selected source."
    result = {"cliHelpExit": help_run.returncode, "cliHelpSha256": hashlib.sha256(help_run.stdout.encode()).hexdigest(),
              "cliDllSha256": digest(cli), "mcpDllSha256": digest(mcp), "cliHelpMatchesDeclaredSource": True,
              "noBackendToolCall": True, "binaryProvenanceLimit": "Explicit local binaries; a release build attestation is not inferred.", "mcpModes": []}
    for effects in (False, True):
        requests = [{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "2025-06-18", "capabilities": {}, "clientInfo": {"name": "inventory", "version": "1"}}},
                    {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}}]
        requests += [{"jsonrpc": "2.0", "id": n + 2, "method": "tools/list", "params": {} if n == 0 else {"cursor": str(n * 20)}} for n in range(26)]
        invocation = ["dotnet", str(mcp), "--base-url", "http://127.0.0.1:1", "--oidc-profile", str(directory / "nonexistent-inventory-profile.json")]
        if effects:
            invocation.append("--enable-effects")
        run = subprocess.run(invocation, input="\n".join(json.dumps(r) for r in requests) + "\n", capture_output=True, text=True, encoding="utf-8", timeout=20, check=True)
        responses = {r["id"]: r for r in map(json.loads, run.stdout.splitlines())}
        protocol = responses[1]["result"]["protocolVersion"]
        tools = []
        page = 0
        while True:
            value = responses[page + 2]["result"]
            tools.extend(value["tools"])
            cursor = value.get("nextCursor")
            if cursor is None:
                break
            page = int(cursor) // 20
        assert len({t["name"] for t in tools}) == len(tools)
        result["mcpModes"].append({"effectsEnabled": effects, "protocolVersion": protocol, "toolCount": len(tools),
                                  "schemaSetSha256": hashlib.sha256(json.dumps(tools, sort_keys=True).encode()).hexdigest(),
                                  "tools": [{"name": t["name"], "effect": not t["annotations"]["readOnlyHint"],
                                             "required": t["inputSchema"].get("required", []),
                                             "properties": sorted(t["inputSchema"].get("properties", {})),
                                             "schemaSha256": hashlib.sha256(json.dumps(t["inputSchema"], sort_keys=True).encode()).hexdigest()} for t in tools]})
    return result

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--compiled-directory", type=pathlib.Path, help="Optional explicitly built matching source outputs; local help/tools-list only.")
    args = parser.parse_args()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    result = collect()
    if args.compiled_directory:
        result["compiledDiscovery"] = observe_compiled(args.compiled_directory.resolve(), result["cliDeclaredUsage"])
    args.output.write_bytes((json.dumps(result, indent=2, ensure_ascii=False) + "\n").encode())
    print(json.dumps({"catalogs": len(result["jsonCatalogs"]), "literalRegistrations": len(result["literalRegistrations"]),
                      "sourceFiles": len(result["sourceFingerprints"]), "exitCodes": result["cliExitCodes"]}))
