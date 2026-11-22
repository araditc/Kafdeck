using System.Reflection;
using System.Security.Cryptography;
using System.Text.Json;
using Kafdeck.Setup;
using Xunit;

namespace Kafdeck.Setup.Tests;

public sealed class SetupTests
{
    [Fact]
    public void BothLanguagesHaveEveryMessageAndEnglishIsDefault()
    {
        var language = new Language(); Assert.False(language.Persian);
        foreach (var key in Language.Keys)
        {
            Assert.False(string.IsNullOrWhiteSpace(language[key]));
            language.Persian = true; Assert.False(string.IsNullOrWhiteSpace(language[key])); language.Persian = false;
        }
        Assert.Contains("Arad ITC", language["about"]); Assert.Contains("https://arad-itc.com", language["about"]);
        Assert.Contains("info@arad-itc.org", language["about"]);
    }

    [Theory]
    [InlineData("../outside.ps1")]
    [InlineData("C:/evil.ps1")]
    [InlineData("/absolute")]
    [InlineData("folder/file:stream")]
    [InlineData("folder/CON.txt")]
    [InlineData("folder/file. ")]
    [InlineData("folder//file")]
    [InlineData("folder/../file")]
    [InlineData("folder\\..\\file")]
    public void WindowsPathAliasesAndTraversalAreRejected(string name) => Assert.Throws<InvalidDataException>(() => SetupMedia.Child(Path.GetTempPath(), name));

    [Fact]
    public void ExactInventoryRejectsChangedBytesAndUnlistedFiles()
    {
        var root = Path.Combine(Path.GetTempPath(), "kafdeck-setup-test-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            var rows = Enumerable.Range(0, 4).Select(i => new { path = $"file{i}.txt", bytes = 4, sha256 = Convert.ToHexString(SHA256.HashData("data"u8)).ToLowerInvariant() }).ToArray();
            foreach (var row in rows) File.WriteAllText(Path.Combine(root, row.path), "data");
            File.WriteAllText(Path.Combine(root, "bundle-manifest.json"), JsonSerializer.Serialize(new { files = rows }));
            SetupMedia.Verify(root);
            File.WriteAllText(Path.Combine(root, "file0.txt"), "evil"); Assert.Throws<InvalidDataException>(() => SetupMedia.Verify(root));
            File.WriteAllText(Path.Combine(root, "file0.txt"), "data");
            File.WriteAllText(Path.Combine(root, "extra.ps1"), "evil"); Assert.Throws<InvalidDataException>(() => SetupMedia.Verify(root));
        }
        finally
        {
            // Only exact files generated in this random test directory; no recursive delete.
            foreach (var file in Directory.EnumerateFiles(root)) File.Delete(file);
            Directory.Delete(root);
        }
    }

    [Fact]
    public void WizardRequiresAcceptanceAndRendersBothDirections()
    {
        Exception? failure = null;
        var thread = new Thread(() =>
        {
            try
            {
                Application.EnableVisualStyles();
                using var form = new Wizard(false);
                form.StartPosition = FormStartPosition.Manual; form.Location = new Point(-4000, -4000);
                form.Show(); Application.DoEvents();
                var binding = BindingFlags.Instance | BindingFlags.NonPublic;
                object Field(string name) => typeof(Wizard).GetField(name, binding)!.GetValue(form)!;
                void Page(int page) { typeof(Wizard).GetField("page", binding)!.SetValue(form, page); typeof(Wizard).GetMethod("Render", binding)!.Invoke(form, null); }
                Assert.Equal(0, ((ComboBox)Field("languages")).SelectedIndex);
                Page(1); Assert.False(((Button)Field("next")).Enabled);
                ((CheckBox)Field("accepted")).Checked = true; Assert.True(((Button)Field("next")).Enabled);
                var directory = Environment.GetEnvironmentVariable("KAFDECK_SETUP_UI_EVIDENCE");
                foreach (var fa in new[] { false, true })
                {
                    ((ComboBox)Field("languages")).SelectedIndex = fa ? 1 : 0;
                    Assert.Equal(fa, form.RightToLeftLayout); Assert.Equal(fa ? RightToLeft.Yes : RightToLeft.No, form.RightToLeft);
                    foreach (var page in new[] { 0, 1, 2, 3 })
                    {
                        Page(page); form.PerformLayout(); Application.DoEvents();
                        if (directory != null)
                        {
                            Directory.CreateDirectory(directory);
                            using var bitmap = new Bitmap(form.Width, form.Height); form.DrawToBitmap(bitmap, new Rectangle(Point.Empty, form.Size));
                            bitmap.Save(Path.Combine(directory, $"{(fa ? "fa" : "en")}-{page}.png"));
                        }
                    }
                }
                using var receipt = JsonDocument.Parse("""{"address":"192.168.100.36","appPort":18080,"identityPort":18443,"databasePort":15433,"networks":["192.168.100.33/32"],"allowedClientNetworks":["192.168.100.0/24"]}""");
                typeof(Wizard).GetMethod("LoadRetainedSettings", binding)!.Invoke(form, [receipt.RootElement]);
                Assert.Equal("192.168.100.36", ((TextBox)Field("address")).Text);
                Assert.Equal("192.168.100.33", ((TextBox)Field("kafka")).Text);
                Assert.Equal(18080, ((NumericUpDown)Field("appPort")).Value);
                Assert.Equal("192.168.100.0/24", ((TextBox)Field("clients")).Text);
            }
            catch (Exception e) { failure = e; }
        });
        thread.SetApartmentState(ApartmentState.STA); thread.Start(); thread.Join();
        if (failure != null) throw new InvalidOperationException("Wizard acceptance/layout failed.", failure);
    }
}
