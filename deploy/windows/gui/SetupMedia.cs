using System.IO.Compression;
using System.Reflection;
using System.Security.AccessControl;
using System.Security.Cryptography;
using System.Security.Principal;
using System.Text.Json;

namespace Kafdeck.Setup;

internal sealed record SetupMetadata(string Version, string PayloadSha256, long PayloadBytes);

internal static class SetupMedia
{
    internal static Stream Resource(string name) => Assembly.GetExecutingAssembly().GetManifestResourceStream("Setup." + name)
        ?? throw new InvalidDataException("Missing build resource.");
    internal static string Text(string name) { using var reader = new StreamReader(Resource(name)); return reader.ReadToEnd(); }
    internal static SetupMetadata Metadata => JsonSerializer.Deserialize<SetupMetadata>(Text("metadata.json"))
        ?? throw new InvalidDataException("Invalid build metadata.");

    // Executed under UAC. No extraction or privileged execution in a user-writable directory.
    internal static void PrivateDirectory(string path)
    {
        for (var parent = new DirectoryInfo(Path.GetFullPath(path)); parent != null; parent = parent.Parent)
            if (parent.Exists && (parent.Attributes & FileAttributes.ReparsePoint) != 0)
                throw new InvalidDataException("A setup directory is a reparse point.");
        var sid = WindowsIdentity.GetCurrent().User!;
        var trusted = new[] { sid, new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null),
            new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null) };
        if (Directory.Exists(path))
        {
            var acl = new DirectoryInfo(path).GetAccessControl();
            if (acl.GetOwner(typeof(SecurityIdentifier)) is not SecurityIdentifier owner || !trusted.Contains(owner))
                throw new InvalidDataException("Untrusted setup directory owner.");
            foreach (FileSystemAccessRule rule in acl.GetAccessRules(true, true, typeof(SecurityIdentifier)))
                if (rule.AccessControlType == AccessControlType.Allow && !trusted.Contains(rule.IdentityReference)
                    && (rule.FileSystemRights & (FileSystemRights.Write | FileSystemRights.Delete | FileSystemRights.ChangePermissions | FileSystemRights.TakeOwnership)) != 0)
                    throw new InvalidDataException("A setup directory is writable by an untrusted account.");
            return;
        }
        var security = new DirectorySecurity();
        security.SetAccessRuleProtection(true, false);
        security.SetOwner(sid);
        foreach (var principal in trusted.Distinct())
            security.AddAccessRule(new FileSystemAccessRule(principal, FileSystemRights.FullControl,
                InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit, PropagationFlags.None, AccessControlType.Allow));
        new DirectoryInfo(path).Create(security);
    }

    internal static string Child(string root, string name)
    {
        var parts = name.Replace('\\', '/').Split('/');
        if (parts.Any(p => p.Length == 0 || p is "." or ".." || p.EndsWith(' ') || p.EndsWith('.')
            || p.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 || p.Contains(':')
            || System.Text.RegularExpressions.Regex.IsMatch(p, @"^(CON|PRN|AUX|NUL|COM[0-9]|LPT[0-9])(?:\.|$)", System.Text.RegularExpressions.RegexOptions.IgnoreCase)))
            throw new InvalidDataException("Unsafe archive path.");
        var path = Path.GetFullPath(Path.Combine(root, Path.Combine(parts)));
        if (!path.StartsWith(Path.GetFullPath(root).TrimEnd(Path.DirectorySeparatorChar) + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
            throw new InvalidDataException("Archive path escapes the cache.");
        return path;
    }

    internal static void Verify(string root)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(Path.Combine(root, "bundle-manifest.json")));
        var expected = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { Path.GetFullPath(Path.Combine(root, "bundle-manifest.json")) };
        foreach (var entry in document.RootElement.GetProperty("files").EnumerateArray())
        {
            var path = Child(root, entry.GetProperty("path").GetString()!);
            if (!expected.Add(path)) throw new InvalidDataException("Duplicate inventory path.");
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read);
            if (stream.Length != entry.GetProperty("bytes").GetInt64()
                || !Convert.ToHexString(SHA256.HashData(stream)).Equals(entry.GetProperty("sha256").GetString(), StringComparison.OrdinalIgnoreCase))
                throw new InvalidDataException("Offline payload verification failed.");
        }
        if (expected.Count < 5 || Directory.EnumerateFiles(root, "*", SearchOption.AllDirectories).Any(p => !expected.Contains(Path.GetFullPath(p))))
            throw new InvalidDataException("Unexpected offline payload files.");
    }

    internal static string Prepare(Action<string> progress)
    {
        var metadata = Metadata;
        if (!System.Text.RegularExpressions.Regex.IsMatch(metadata.PayloadSha256, "^[0-9a-f]{64}$")) throw new InvalidDataException();
        var company = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ProgramFiles), "Arad ITC");
        PrivateDirectory(company);
        var product = Path.Combine(company, "Kafdeck Setup");
        PrivateDirectory(product);
        var cache = Path.Combine(product, metadata.PayloadSha256);
        PrivateDirectory(cache);
        // Serializes distinct copies of the same installer as well as reboot/resume.
        using var cacheLock = new FileStream(Path.Combine(cache, "cache.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        var media = Path.Combine(cache, "media");
        if (Directory.Exists(media))
        {
            PrivateDirectory(media);
            Verify(media); // Interrupted extraction fails closed; preserve it for diagnosis.
        }
        else
        {
            var staging = Path.Combine(cache, "staging-" + Guid.NewGuid().ToString("N"));
            PrivateDirectory(staging);
            var archivePath = Path.Combine(staging, "payload.zip");
            using (var payload = Resource("payload.zip"))
            using (var output = new FileStream(archivePath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                payload.CopyTo(output);
            using (var input = File.OpenRead(archivePath))
                if (input.Length != metadata.PayloadBytes || !Convert.ToHexString(SHA256.HashData(input)).Equals(metadata.PayloadSha256, StringComparison.OrdinalIgnoreCase))
                    throw new InvalidDataException("Embedded payload verification failed.");
            var extracted = Path.Combine(staging, "media");
            PrivateDirectory(extracted);
            using (var archive = ZipFile.OpenRead(archivePath))
            {
                if (archive.Entries.Count > 100_000 || archive.Entries.Sum(e => e.Length) > 8L * 1024 * 1024 * 1024)
                    throw new InvalidDataException("Archive exceeds installation bounds.");
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var entry in archive.Entries)
                {
                    if (entry.FullName.EndsWith('/')) continue;
                    if (((entry.ExternalAttributes >> 16) & 0xf000) == 0xa000) throw new InvalidDataException("Archive contains a link.");
                    var path = Child(extracted, entry.FullName);
                    if (!seen.Add(path)) throw new InvalidDataException("Duplicate archive entry.");
                    Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                    using var source = entry.Open();
                    using var target = new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None);
                    source.CopyTo(target);
                }
            }
            Verify(extracted);
            Directory.Move(extracted, media);
            File.Delete(archivePath); // Exact file generated in this protected staging directory.
            Directory.Delete(staging); // Empty only; never recursive.
        }
        progress("verified");
        File.WriteAllText(Path.Combine(cache, "Setup-Worker.ps1"), Text("worker.ps1"), new System.Text.UTF8Encoding(true));
        var sourceExe = Environment.ProcessPath!;
        var installedExe = Path.Combine(cache, "Kafdeck.Setup.exe");
        if (!sourceExe.Equals(installedExe, StringComparison.OrdinalIgnoreCase))
        {
            File.Copy(sourceExe, installedExe + ".new", true);
            File.Move(installedExe + ".new", installedExe, true);
        }
        return cache;
    }
}
