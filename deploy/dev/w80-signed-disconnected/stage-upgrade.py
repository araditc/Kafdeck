"""Stage actual immutable published v0.8 image bytes; compile only fixture tooling."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts/release"))
from v09_prepublication import digest, require, write

private, evidence = Path(sys.argv[1]), Path(sys.argv[2])
old = "ghcr.io/araditc/kafdeck@sha256:7b64f67b7f050b3c8cb5c2678b6ce94f6886e854cebfca6d6730170164fedb45"
subprocess.run(["docker", "pull", old], check=True)
subprocess.run(["cosign", "verify", "--certificate-identity",
    "https://github.com/araditc/Kafdeck/.github/workflows/publish-release.yml@refs/heads/main",
    "--certificate-oidc-issuer", "https://token.actions.githubusercontent.com",
    "--certificate-github-workflow-sha", "cbedd025ab97db10fb21407ea5188c08d6a4d8ce", old], check=True, stdout=subprocess.DEVNULL)
container = subprocess.check_output(["docker", "create", "--network", "none", old], text=True).strip()
try:
    subprocess.run(["docker", "cp", container + ":/app", str(private / "released-v08")], check=True)
    subprocess.run(["docker", "cp", container + ":/usr/share/dotnet", str(private / "old-dotnet")], check=True)
finally:
    subprocess.run(["docker", "rm", container], check=True)
source = private / "probe-src"
source.mkdir()
shutil.copyfile(ROOT / "deploy/dev/w80-signed-disconnected/UpgradeProbe.cs", source / "Program.cs")
references = "".join('<Reference Include="' + escape(path.stem) + '"><HintPath>' + escape(str(path)) + '</HintPath></Reference>'
                     for path in sorted((private / "released-v08").glob("*.dll")))
(source / "UpgradeProbe.csproj").write_text('<Project Sdk="Microsoft.NET.Sdk"><PropertyGroup><OutputType>Exe</OutputType>'
    '<TargetFramework>net10.0</TargetFramework><ImplicitUsings>enable</ImplicitUsings><Nullable>enable</Nullable>'
    '</PropertyGroup><ItemGroup><FrameworkReference Include="Microsoft.AspNetCore.App"/>' + references + '</ItemGroup></Project>')
subprocess.run(["dotnet", "build", str(source / "UpgradeProbe.csproj"), "-c", "Release", "-o", str(private / "probe")], check=True)
# Preserve official old dependency bytes/native assets for the fixture probe;
# this does not rebuild either product binary.
shutil.copytree(private / "released-v08", private / "probe", dirs_exist_ok=True)
for library in (private / "released-v08").rglob("*.so"):
    shutil.copyfile(library, private / "probe" / library.name)
write(evidence / "upgrade-input.json", {"schemaVersion": 1, "baselineSourceSha": "cbedd025ab97db10fb21407ea5188c08d6a4d8ce",
    "officialPublishedV08Image": old, "officialPublishedV08SignatureVerified": True,
    "oldApiAssemblySha256": digest(private / "released-v08/Kafdeck.Api.dll"),
    "oldRuntimeHostSha256": digest(private / "old-dotnet/dotnet"),
    "fixtureProbeSourceSha256": digest(source / "Program.cs"), "fixtureProbeAssemblySha256": digest(private / "probe/UpgradeProbe.dll"),
    "fixtureOnlyCompiled": True, "oldProductRebuilt": False, "newProductRebuilt": False,
    "driverSourceFiles": {path.name: digest(path) for path in sorted((ROOT / "deploy/dev/w80-signed-disconnected").glob("*")) if path.is_file()}})
