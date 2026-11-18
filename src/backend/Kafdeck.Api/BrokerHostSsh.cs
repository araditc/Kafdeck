using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Kafdeck.Infrastructure.Configuration;
using Renci.SshNet;

namespace Kafdeck.Api;

internal interface IBrokerHostProbe
{
    Task<BrokerHostObservation> ObserveAsync(BrokerHostDraft host, CancellationToken cancellation);
}

internal sealed class BrokerHostSsh(BrokerHostOptions options) : IBrokerHostProbe
{
    private readonly SemaphoreSlim _slots = new(4);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    public async Task<BrokerHostObservation> ObserveAsync(BrokerHostDraft host, CancellationToken cancellation)
    {
        if (!BrokerHostValidation.Draft(host, options)) throw new BrokerHostFailure("host_configuration_invalid");
        if (!await _slots.WaitAsync(0, cancellation)) throw new BrokerHostFailure("host_probe_busy");
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        try
        {
            return await ConnectAsync(host, timeout.Token);
        }
        catch (BrokerHostFailure) { throw; }
        catch (Exception) when (!cancellation.IsCancellationRequested)
        {
            throw new BrokerHostFailure("host_probe_unavailable");
        }
        finally { _slots.Release(); }
    }

    private async Task<BrokerHostObservation> ConnectAsync(BrokerHostDraft host, CancellationToken cancellation)
    {
        var binding = options.Credentials.Single(value => value.Id == host.CredentialId);
        var resolver = new SecretResolver();
        var secret = resolver.ResolveBounded(SecretReference.Parse(binding.Secret), 65536).Reveal();
        using var keyStream = new MemoryStream(Encoding.UTF8.GetBytes(secret));
        using var key = binding.Authentication == "privateKey"
            ? new PrivateKeyFile(keyStream, binding.Passphrase is null ? null :
                resolver.ResolveBounded(SecretReference.Parse(binding.Passphrase), 4096).Reveal()) : null;
        using AuthenticationMethod authentication = key is null
            ? new PasswordAuthenticationMethod(host.User, secret)
            : new PrivateKeyAuthenticationMethod(host.User, key);
        var connection = new Renci.SshNet.ConnectionInfo(host.Address, host.Port, host.User, authentication)
        { Timeout = TimeSpan.FromSeconds(8) };
        using var client = new SshClient(connection);
        var mismatched = false;
        client.HostKeyReceived += (_, keyEvent) =>
        {
            var actual = "SHA256:" + Convert.ToBase64String(SHA256.HashData(keyEvent.HostKey)).TrimEnd('=');
            keyEvent.CanTrust = CryptographicOperations.FixedTimeEquals(Encoding.ASCII.GetBytes(actual), Encoding.ASCII.GetBytes(host.Fingerprint));
            mismatched |= !keyEvent.CanTrust;
        };
        try { await client.ConnectAsync(cancellation); }
        catch (Exception) when (mismatched) { throw new BrokerHostFailure("host_key_mismatch"); }
        using var command = client.CreateCommand("kafdeck-host-probe-v1");
        command.CommandTimeout = TimeSpan.FromSeconds(12);
        // Stream both channels with one shared cancellation deadline. A remote
        // helper cannot force unbounded output into a command Result string.
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
        var execution = command.ExecuteAsync(bounded.Token);
        var stdout = ReadBounded(command.OutputStream, bounded);
        var stderr = ReadBounded(command.ExtendedOutputStream, bounded);
        try
        {
            await Task.WhenAll(execution, stdout, stderr);
            if (command.ExitStatus != 0) throw new BrokerHostFailure("host_helper_unavailable");
            var observation = JsonSerializer.Deserialize<BrokerHostObservation>(await stdout, Json);
            if (observation is null || observation.Version != 1 || observation.Targets is null || observation.Targets.Length > 32 ||
                observation.Targets.Any(target => !ValidTarget(target)) ||
                observation.Targets.Select(target => target.Id).Distinct().Count() != observation.Targets.Length)
                throw new BrokerHostFailure("host_identity_invalid");
            return observation;
        }
        finally { client.Disconnect(); }
    }

    private static bool ValidTarget(BrokerServiceTarget? target) => target is not null && BrokerHostValidation.Name(target.Id) &&
        (target.Adapter == "systemd" ? BrokerHostValidation.Name(target.Unit) && target.Unit!.EndsWith(".service", StringComparison.Ordinal) :
            target.Adapter == "compose" && BrokerHostValidation.Name(target.Project) && BrokerHostValidation.Name(target.Service)) &&
        target.Status is "verified" or "identity_unavailable" &&
        (target.Status != "verified" || target.BrokerId >= 0 && target.KafkaClusterId is { Length: > 0 and <= 128 } &&
            target.Instance is { Length: > 0 and <= 160 } && !target.KafkaClusterId.Any(char.IsControl) && !target.Instance.Any(char.IsControl));

    private static async Task<byte[]> ReadBounded(Stream stream, CancellationTokenSource bounded)
    {
        using var output = new MemoryStream();
        var buffer = new byte[4096];
        int length;
        while ((length = await stream.ReadAsync(buffer, bounded.Token)) != 0)
        {
            if (output.Length + length > 65536)
            {
                await bounded.CancelAsync();
                throw new BrokerHostFailure("host_output_limit");
            }
            output.Write(buffer, 0, length);
        }
        return output.ToArray();
    }
}
