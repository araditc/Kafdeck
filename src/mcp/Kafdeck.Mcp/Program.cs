using System.Text;
using Kafdeck.Cli;
using Kafdeck.Mcp;

Console.InputEncoding = new UTF8Encoding(false, true);
Console.OutputEncoding = new UTF8Encoding(false);
try
{
    var effects = args.Contains("--enable-effects", StringComparer.Ordinal);
    var options = args.Where(x => x != "--enable-effects").ToArray();
    if (options.Length != 4 || options[0] != "--base-url" || options[2] is not ("--oidc-profile" or "--service-profile"))
        throw new CliUsageException("Use --base-url <origin> (--oidc-profile <file> | --service-profile <file>) [--enable-effects].");
    options[0] = "--url";
    var invocation = CliParser.Parse([.. options, "capabilities", "mutations"]);
    using var backend = new McpApiBackend(invocation);
    var server = new McpServer(backend, effects);
    using var shutdown = new CancellationTokenSource();
    Console.CancelKeyPress += (_, e) => { e.Cancel = true; shutdown.Cancel(); };
    await server.RunAsync(Console.In, Console.Out, shutdown.Token);
    return 0;
}
catch (OperationCanceledException) { return 0; }
catch (Exception)
{
    // Exception text may contain a credential, profile path or remote body.
    await Console.Error.WriteLineAsync("MCP startup or transport failed; verify the operator-owned origin and OIDC profile.");
    return 2;
}
