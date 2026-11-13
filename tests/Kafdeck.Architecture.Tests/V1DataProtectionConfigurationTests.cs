using Kafdeck.Api;
using Kafdeck.Infrastructure.Configuration;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Kafdeck.Architecture.Tests;

public sealed class V1DataProtectionConfigurationTests
{
    [Fact]
    public void Unconfigured_host_retains_its_legacy_provider_configuration()
    {
        var services=new ServiceCollection();services.ConfigureManagedStateDataProtection(new ConfigurationBuilder().Build());
        Assert.Empty(services);
    }
    [Theory]
    [InlineData("relative")]
    [InlineData("missing")]
    [InlineData("application-only")]
    public void Invalid_restore_configuration_is_rejected_before_host_start(string kind)
    {
        var settings=new Dictionary<string,string?> { ["Kafdeck:DataProtection:KeyDirectory"]=kind=="relative"?"relative":kind=="missing"?Path.Combine(Path.GetTempPath(),Guid.NewGuid().ToString("N")):null,
            ["Kafdeck:DataProtection:ApplicationName"]=kind=="application-only"?"context":null };
        Assert.Throws<KafdeckConfigurationException>(()=>new ServiceCollection().ConfigureManagedStateDataProtection(new ConfigurationBuilder().AddInMemoryCollection(settings).Build()));
    }
    [Fact]
    public void Explicit_matching_keyring_and_application_context_reopen_the_same_protected_value()
    {
        using var f=new ManagedProvisioningFixture();var directory=Path.Combine(f.DirectoryPath,"keys");Directory.CreateDirectory(directory);
        IServiceProvider Provider(string name)
        { var services=new ServiceCollection();services.AddLogging();services.ConfigureManagedStateDataProtection(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string,string?> { ["Kafdeck:DataProtection:KeyDirectory"]=directory,["Kafdeck:DataProtection:ApplicationName"]=name }).Build());return services.BuildServiceProvider(); }
        using var first=(ServiceProvider)Provider("g06-configured");var cipher=first.GetRequiredService<IDataProtectionProvider>().CreateProtector("credential").Protect("fixture-only-value");
        using var reopened=(ServiceProvider)Provider("g06-configured");Assert.Equal("fixture-only-value",reopened.GetRequiredService<IDataProtectionProvider>().CreateProtector("credential").Unprotect(cipher));
        using var wrong=(ServiceProvider)Provider("wrong");Assert.Throws<System.Security.Cryptography.CryptographicException>(()=>wrong.GetRequiredService<IDataProtectionProvider>().CreateProtector("credential").Unprotect(cipher));
        if(OperatingSystem.IsWindows())Assert.Contains("DpapiXml",File.ReadAllText(Directory.GetFiles(directory,"key-*.xml").Single()),StringComparison.Ordinal);
    }
}
