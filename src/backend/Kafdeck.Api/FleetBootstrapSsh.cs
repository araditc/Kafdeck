using System.Security.Cryptography;
using System.Text;
using Renci.SshNet;

namespace Kafdeck.Api;

internal sealed record FleetBootstrapConnection(string Address, int Port, string User, string Fingerprint,
    string Authentication, string Secret, string? Passphrase = null, string? SudoPassword = null);

internal interface IFleetBootstrapSsh
{
    Task<byte[]> CallAsync(FleetBootstrapConnection connection, byte[] payload, CancellationToken cancellation);
}

internal sealed class FleetBootstrapSsh : IFleetBootstrapSsh
{
    private readonly SemaphoreSlim _slots = new(2);

    internal static bool Valid(FleetBootstrapConnection? connection, BrokerHostOptions options) => connection is not null &&
        options.Current.Enabled && options.Allows(connection.Address) && connection.Port is >= 1 and <= 65535 &&
        BrokerHostValidation.Name(connection.User) && BrokerHostValidation.Fingerprint(connection.Fingerprint) &&
        connection.Authentication is "privateKey" or "password" && connection.Secret is { Length: > 0 and <= 65536 } &&
        !connection.Secret.Contains('\0') && connection.Passphrase is not { Length: > 4096 } &&
        connection.SudoPassword is not { Length: > 4096 } && connection.SudoPassword?.Any(char.IsControl) != true;

    public async Task<byte[]> CallAsync(FleetBootstrapConnection connection, byte[] payload, CancellationToken cancellation)
    {
        if (!await _slots.WaitAsync(TimeSpan.FromSeconds(2), cancellation)) throw new BrokerHostFailure("host_probe_busy");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        timeout.CancelAfter(TimeSpan.FromSeconds(120));
        try
        {
            using var stream = new MemoryStream(Encoding.UTF8.GetBytes(connection.Secret));
            using var key = connection.Authentication == "privateKey" ? new PrivateKeyFile(stream, connection.Passphrase) : null;
            using AuthenticationMethod authentication = key is null
                ? new PasswordAuthenticationMethod(connection.User, connection.Secret)
                : new PrivateKeyAuthenticationMethod(connection.User, key);
            var information = new Renci.SshNet.ConnectionInfo(connection.Address, connection.Port, connection.User, authentication)
            { Timeout = TimeSpan.FromSeconds(10) };
            using var client = new SshClient(information);
            var mismatched = false;
            client.HostKeyReceived += (_, hostKey) =>
            {
                var actual = "SHA256:" + Convert.ToBase64String(SHA256.HashData(hostKey.HostKey)).TrimEnd('=');
                hostKey.CanTrust = CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(actual),
                    Encoding.ASCII.GetBytes(connection.Fingerprint));
                mismatched |= !hostKey.CanTrust;
            };
            try { await client.ConnectAsync(timeout.Token); }
            catch (Exception) when (mismatched) { throw new BrokerHostFailure("host_key_mismatch"); }
            var encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(FleetBootstrapResources.Text("bootstrap.py")));
            var python = "/usr/bin/python3 -I -c 'import base64;exec(compile(base64.b64decode(\"" + encoded + "\"),\"<kafdeck-bootstrap>\",\"exec\"))'";
            var sudo = connection.User != "root";
            var needsPassword = false;
            if (sudo)
            {
                using var privilege = client.CreateCommand("sudo -n -- /usr/bin/true");
                privilege.CommandTimeout = TimeSpan.FromSeconds(5);
                await privilege.ExecuteAsync(timeout.Token);
                needsPassword = privilege.ExitStatus != 0;
                if (needsPassword && string.IsNullOrEmpty(connection.SudoPassword))
                    throw new BrokerHostFailure("bootstrap_sudo_password_required");
            }
            using var command = client.CreateCommand((sudo ? needsPassword ? "sudo -S -p '' " : "sudo -n " : "") + python);
            command.CommandTimeout = TimeSpan.FromSeconds(110);
            var execution = command.ExecuteAsync(timeout.Token);
            var stdout = Bounded(command.OutputStream, timeout.Token);
            var stderr = Bounded(command.ExtendedOutputStream, timeout.Token);
            try
            {
                using (var input = command.CreateInputStream())
                {
                    if (needsPassword) await input.WriteAsync(Encoding.UTF8.GetBytes((connection.SudoPassword ?? "") + "\n"), timeout.Token);
                    await input.WriteAsync(payload, timeout.Token);
                }
                await Task.WhenAll(execution, stdout, stderr);
                if (command.ExitStatus != 0) throw new BrokerHostFailure("bootstrap_privilege_or_helper_unavailable");
                return await stdout;
            }
            finally
            {
                await timeout.CancelAsync();
                client.Disconnect();
                try { await Task.WhenAll(execution, stdout, stderr); }
                catch (Exception) { /* The original failure is returned without remote output. */ }
            }
        }
        catch (BrokerHostFailure) { throw; }
        catch (Exception) when (!cancellation.IsCancellationRequested) { throw new BrokerHostFailure("bootstrap_connection_unavailable"); }
        finally { _slots.Release(); }
    }

    private static async Task<byte[]> Bounded(Stream input, CancellationToken cancellation)
    {
        using var output = new MemoryStream();
        var buffer = new byte[4096];
        int length;
        while ((length = await input.ReadAsync(buffer, cancellation)) != 0)
        {
            if (output.Length + length > 65536) throw new BrokerHostFailure("bootstrap_output_limit");
            output.Write(buffer, 0, length);
        }
        return output.ToArray();
    }
}

internal static class FleetBootstrapResources
{
    internal static string Text(string name)
    {
        using var input = typeof(FleetBootstrapResources).Assembly.GetManifestResourceStream("Kafdeck.Fleet.Host." + name)
            ?? throw new InvalidOperationException("Pinned host resource is missing.");
        using var reader = new StreamReader(input);
        return reader.ReadToEnd().Replace("\r\n", "\n", StringComparison.Ordinal);
    }

    internal static Dictionary<string, string> Helpers() => new[] { "probe.py", "ssh-entry.sh", "agent-lifecycle.py", "agent-entry.sh" }
        .ToDictionary(name => name, name => Convert.ToBase64String(Encoding.UTF8.GetBytes(Text(name))));

    internal static byte[] Agent()
    {
        using var input = typeof(FleetBootstrapResources).Assembly.GetManifestResourceStream("Kafdeck.Monitoring.Agent.jar")
            ?? throw new InvalidOperationException("Bundled agent is missing.");
        using var output = new MemoryStream();
        input.CopyTo(output);
        return output.ToArray();
    }

    internal static string Version => typeof(FleetBootstrapResources).Assembly.GetName().Version is { } version
        ? $"{version.Major}.{version.Minor}.{version.Build}" : "1.1.0";

    internal static (string Private, string Public) KeyPair()
    {
        using var key = RSA.Create(3072);
        var parameters = key.ExportParameters(false);
        using var blob = new MemoryStream();
        Write(Encoding.ASCII.GetBytes("ssh-rsa"), false);
        Write(parameters.Exponent!, true);
        Write(parameters.Modulus!, true);
        return (key.ExportRSAPrivateKeyPem(), "ssh-rsa " + Convert.ToBase64String(blob.ToArray()));

        void Write(byte[] bytes, bool integer)
        {
            var prefix = integer && (bytes[0] & 0x80) != 0;
            Span<byte> length = stackalloc byte[4];
            System.Buffers.Binary.BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length + (prefix ? 1 : 0));
            blob.Write(length);
            if (prefix) blob.WriteByte(0);
            blob.Write(bytes);
        }
    }
}
