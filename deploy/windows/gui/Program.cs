using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Principal;
using System.Text.Json;
using Microsoft.Win32;

namespace Kafdeck.Setup;

#if !SETUP_TESTS
internal static class Program
{
    [STAThread]
    private static void Main(string[] args)
    {
        ApplicationConfiguration.Initialize();
        using var single = new Mutex(true, @"Global\AradITC.Kafdeck.Setup", out var created);
        if (!created) { MessageBox.Show("Kafdeck Setup is already running. / نصب‌کننده در حال اجراست."); return; }
        Application.Run(new Wizard(args.Contains("--remove", StringComparer.Ordinal)));
    }
}
#endif

internal sealed class Wizard : Form
{
    private readonly Language language = new();
    private readonly Panel content = new() { Dock = DockStyle.Fill, Padding = new Padding(24), AutoScroll = true };
    private readonly Label heading = new() { Dock = DockStyle.Fill, AutoSize = false, Font = new Font("Segoe UI", 18, FontStyle.Bold), TextAlign = ContentAlignment.MiddleLeft };
    private readonly Button next = new() { AutoSize = true, MinimumSize = new Size(110, 38) };
    private readonly Button back = new() { AutoSize = true, MinimumSize = new Size(110, 38) };
    private readonly ComboBox languages = new() { DropDownStyle = ComboBoxStyle.DropDownList, Width = 220 };
    private readonly ComboBox action = new() { DropDownStyle = ComboBoxStyle.DropDownList, Dock = DockStyle.Top };
    private readonly TextBox root = new() { Text = @"C:\Kafdeck" };
    private readonly TextBox address = new() { Text = "127.0.0.1" };
    private readonly TextBox kafka = new();
    private readonly TextBox clients = new() { Text = "LocalSubnet" };
    private readonly TextBox password = new() { UseSystemPasswordChar = true, MaxLength = 1024 };
    private readonly NumericUpDown appPort = Port(8080), identityPort = Port(8443), databasePort = Port(15432);
    private readonly CheckBox accepted = new() { AutoSize = true, MaximumSize = new Size(800, 100) };
    private readonly Label status = new() { AutoSize = true, MaximumSize = new Size(800, 200) };
    private int page;
    private bool busy;
    private bool rendering;
    private string outcome = "failed", origin = "", code = "", resultRoot = "";
    private bool firstAdmin;
    private readonly string version;
    private static NumericUpDown Port(int value) => new() { Minimum = 1024, Maximum = 65535, Value = value };

    public Wizard(bool remove)
    {
        Text = "Kafdeck Setup — Arad ITC";
        Font = new Font("Segoe UI", 10);
        AutoScaleMode = AutoScaleMode.Dpi;
        MinimumSize = new Size(900, 680); Size = new Size(1000, 780); StartPosition = FormStartPosition.CenterScreen;
        try { version = SetupMedia.Metadata.Version; } catch (InvalidDataException) { version = "development"; }
        var shell = new TableLayoutPanel { Dock = DockStyle.Fill, RowCount = 4, ColumnCount = 1 };
        shell.RowStyles.Add(new RowStyle(SizeType.Absolute, 100)); shell.RowStyles.Add(new RowStyle(SizeType.Percent, 100));
        shell.RowStyles.Add(new RowStyle(SizeType.Absolute, 42)); shell.RowStyles.Add(new RowStyle(SizeType.Absolute, 64));
        var header = new TableLayoutPanel { Dock = DockStyle.Fill, Padding = new Padding(24, 10, 24, 10), ColumnCount = 2, BackColor = Color.White };
        header.ColumnStyles.Add(new ColumnStyle(SizeType.Absolute, 85)); header.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100));
        var logo = new PictureBox { Dock = DockStyle.Fill, SizeMode = PictureBoxSizeMode.Zoom };
        try { using var stream = SetupMedia.Resource("logo.png"); using var image = Image.FromStream(stream); logo.Image = new Bitmap(image); } catch (InvalidDataException) { }
        header.Controls.Add(logo); header.Controls.Add(heading);
        shell.Controls.Add(header, 0, 0); shell.Controls.Add(content, 0, 1);
        var contact = new FlowLayoutPanel { Dock = DockStyle.Fill, FlowDirection = FlowDirection.LeftToRight, WrapContents = false, Padding = new Padding(24, 5, 24, 0) };
        contact.Controls.Add(new Label { Text = "Arad ITC", AutoSize = true });
        var website = new LinkLabel { Text = "https://arad-itc.com", AutoSize = true, Margin = new Padding(20, 0, 20, 0) };
        website.LinkClicked += (_, _) => Open("https://arad-itc.com"); contact.Controls.Add(website);
        var email = new LinkLabel { Text = "info@arad-itc.org", AutoSize = true }; email.LinkClicked += (_, _) => Open("mailto:info@arad-itc.org"); contact.Controls.Add(email);
        shell.Controls.Add(contact, 0, 2);
        var navigation = new FlowLayoutPanel { Dock = DockStyle.Fill, FlowDirection = FlowDirection.RightToLeft, Padding = new Padding(20, 10, 20, 10) };
        next.Click += async (_, _) => { if (page == 5) Close(); else if (page == 3) await Execute(); else if (page != 2 || ValidateSettings()) { page++; Render(); } };
        back.Click += (_, _) => { page--; Render(); };
        navigation.Controls.Add(next); navigation.Controls.Add(back); shell.Controls.Add(navigation, 0, 3); Controls.Add(shell);
        languages.Items.AddRange(["English", "فارسی"]); languages.SelectedIndex = 0;
        languages.SelectedIndexChanged += (_, _) => { language.Persian = languages.SelectedIndex == 1; Render(); };
        accepted.CheckedChanged += (_, _) => { if (page == 1) next.Enabled = accepted.Checked; };
        using var registration = Registry.LocalMachine.OpenSubKey(@"Software\Microsoft\Windows\CurrentVersion\Uninstall\AradITC.Kafdeck");
        if (registration?.GetValue("InstallLocation") is string installed) { root.Text = installed; root.ReadOnly = true; }
        var receiptPath = Path.Combine(root.Text, "config", "offline-install.json");
        var initial = 0;
        if (File.Exists(receiptPath))
        {
            try { using var doc = JsonDocument.Parse(File.ReadAllText(receiptPath)); initial = doc.RootElement.GetProperty("phase").GetString() == "complete" ? 1 : 0; LoadRetainedSettings(doc.RootElement); }
            catch (Exception e) when (e is IOException or JsonException or KeyNotFoundException) { }
        }
        action.Items.AddRange([language["install"], language["upgrade"], language["remove"]]); action.SelectedIndex = remove ? 2 : initial;
        action.SelectedIndexChanged += (_, _) => { if (page == 2) Render(); };
        root.Leave += (_, _) =>
        {
            var path = Path.Combine(root.Text, "config", "offline-install.json");
            if (!File.Exists(path)) return;
            try
            {
                using var doc = JsonDocument.Parse(File.ReadAllText(path)); LoadRetainedSettings(doc.RootElement);
                if (action.SelectedIndex != 2) action.SelectedIndex = doc.RootElement.GetProperty("phase").GetString() == "complete" ? 1 : 0;
            }
            catch (Exception e) when (e is IOException or JsonException or KeyNotFoundException) { }
        };
        FormClosing += (_, e) => { if (busy) e.Cancel = true; };
        Render();
    }

    private void LoadRetainedSettings(JsonElement receipt)
    {
        if (receipt.TryGetProperty("address", out var host)) address.Text = host.GetString() ?? "";
        foreach (var (key, field) in new[] { ("appPort", appPort), ("identityPort", identityPort), ("databasePort", databasePort) })
            if (receipt.TryGetProperty(key, out var port) && port.TryGetInt32(out var value) && value is >= 1024 and <= 65535) field.Value = value;
        if (receipt.TryGetProperty("networks", out var networks)) kafka.Text = string.Join(",", networks.EnumerateArray().Select(n => n.GetString()?.Replace("/32", "", StringComparison.Ordinal)));
        if (receipt.TryGetProperty("allowedClientNetworks", out var allowed)) clients.Text = string.Join(",", allowed.EnumerateArray().Select(n => n.GetString()));
    }

    private void Render()
    {
        if (rendering) return;
        rendering = true;
        SuspendLayout();
        try
        {
        RightToLeft = language.Persian ? RightToLeft.Yes : RightToLeft.No;
        RightToLeftLayout = language.Persian;
        // Controls carrying input survive page switches; no credentials go to browser/storage/config files.
        content.Controls.Clear();
        var layout = new TableLayoutPanel { Dock = DockStyle.Top, AutoSize = true, ColumnCount = 1, Padding = new Padding(0) };
        layout.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 100)); content.Controls.Add(layout);
        void Add(Control c) { c.Margin = new Padding(0, 0, 0, 14); c.Dock = DockStyle.Top; layout.Controls.Add(c); }
        void Paragraph(string key) => Add(new Label { Text = language[key], AutoSize = true, MaximumSize = new Size(Math.Max(600, content.ClientSize.Width - 65), 0) });
        void Field(string key, Control control) { Add(new Label { Text = language[key], AutoSize = true }); control.RightToLeft = RightToLeft.No; Add(control); }
        heading.Text = language[page switch { 0 => "welcome", 1 => "agreement", 2 => "configuration", 3 => "review", 4 => "title", _ => outcome }] + " — " + version;
        next.Text = language[page switch { 3 => "start", 5 => "close", _ => "next" }]; back.Text = language["back"];
        next.Enabled = !busy && (page != 1 || accepted.Checked); back.Enabled = !busy && page is > 0 and < 4;
        if (page == 0) { Paragraph("intro"); Paragraph("accountNotice"); Field("language", languages); }
        else if (page == 1)
        {
            Paragraph("terms");
            var tabs = new TabControl { Height = 300, Dock = DockStyle.Top };
            foreach (var (title, file) in new[] { (language["agreement"], "agreement.txt"), (language["notices"], "notices.txt") })
            {
                var tab = new TabPage(title); var text = new TextBox { Multiline = true, ReadOnly = true, ScrollBars = ScrollBars.Vertical, Dock = DockStyle.Fill, RightToLeft = RightToLeft.No };
                try { text.Text = SetupMedia.Text(file).Replace("\n", Environment.NewLine); } catch (InvalidDataException) { text.Text = language["missing"]; }
                tab.Controls.Add(text); tabs.TabPages.Add(tab);
            }
            Add(tabs); accepted.Text = language["accept"]; Add(accepted);
        }
        else if (page == 2)
        {
            var index = action.SelectedIndex; action.Items[0] = language["install"]; action.Items[1] = language["upgrade"]; action.Items[2] = language["remove"]; action.SelectedIndex = index;
            var fields = new TableLayoutPanel { Dock = DockStyle.Top, AutoSize = true, ColumnCount = 2 };
            fields.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 38)); fields.ColumnStyles.Add(new ColumnStyle(SizeType.Percent, 62));
            void Setting(string key, Control control)
            {
                var row = fields.RowCount++; fields.RowStyles.Add(new RowStyle(SizeType.AutoSize));
                fields.Controls.Add(new Label { Text = language[key], AutoSize = true, MaximumSize = new Size(320, 0), Margin = new Padding(0, 4, 16, 14) }, 0, row);
                control.Dock = DockStyle.Top; control.Margin = new Padding(0, 4, 0, 14); control.RightToLeft = control == action ? RightToLeft : RightToLeft.No;
                fields.Controls.Add(control, 1, row);
            }
            Setting("action", action); Setting("root", root);
            if (index == 0)
            {
                Setting("address", address); Setting("kafka", kafka); Setting("clients", clients);
                Setting("appPort", appPort); Setting("identityPort", identityPort); Setting("databasePort", databasePort);
                Setting("account", new TextBox { Text = WindowsIdentity.GetCurrent().Name, ReadOnly = true });
                Setting("password", password);
            }
            Add(fields);
            if (index == 0) Paragraph("passwordHelp");
        }
        else if (page == 3)
        {
            Add(new Label { AutoSize = true, Text = action.Text + Environment.NewLine + root.Text, RightToLeft = RightToLeft.No }); Paragraph("reviewText");
        }
        else if (page == 4)
        {
            Paragraph("working"); Add(new ProgressBar { Style = ProgressBarStyle.Marquee, Height = 24 }); Add(status);
        }
        else
        {
            if (outcome == "complete")
            {
                Paragraph(firstAdmin ? "firstAdmin" : "login");
                Add(new TextBox { Text = origin + "/", ReadOnly = true, RightToLeft = RightToLeft.No });
                if (firstAdmin && code.Length > 0) { Field("code", new TextBox { Text = code, ReadOnly = true }); }
                var open = new Button { Text = language["open"], AutoSize = true }; open.Click += (_, _) => Open(origin + "/"); Add(open);
            }
            else Paragraph(outcome switch { "reboot" => "rebootHelp", "removed" => "removedHelp", "recovered" => "recoveredHelp", _ => "failedHelp" });
            if (resultRoot.Length > 0) Add(new TextBox { Text = resultRoot, ReadOnly = true, RightToLeft = RightToLeft.No });
        }
        }
        finally { ResumeLayout(true); rendering = false; }
    }

    internal static bool V4(string text) => IPAddress.TryParse(text, out var ip) && ip.AddressFamily == AddressFamily.InterNetwork;
    private static string[] Split(string value) => value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    private bool ValidateSettings()
    {
        var valid = Path.IsPathFullyQualified(root.Text) && !root.Text.Any(c => c > 127 || char.IsWhiteSpace(c)) && root.Text.TrimEnd('\\').Length > 2;
        if (action.SelectedIndex == 0)
            valid &= V4(address.Text) && Split(kafka.Text).Length > 0 && Split(kafka.Text).All(V4)
                && Split(clients.Text).Length > 0 && Split(clients.Text).All(n => n == "LocalSubnet" || V4(n) ||
                    (n.Split('/') is var parts && parts.Length == 2 && V4(parts[0]) && int.TryParse(parts[1], out var bits) && bits is >= 0 and <= 32))
                && password.Text.Length > 0 && new[] { appPort.Value, identityPort.Value, databasePort.Value }.Distinct().Count() == 3;
        if (!valid) MessageBox.Show(this, language["invalid"], language["title"], MessageBoxButtons.OK, MessageBoxIcon.Warning);
        return valid;
    }

    private async Task Execute()
    {
        if (!accepted.Checked) return;
        busy = true; page = 4; status.Text = language["verifying"]; Render();
        object? request = new { action = new[] { "install", "upgrade", "remove" }[action.SelectedIndex], root = root.Text, address = address.Text,
            kafkaHosts = Split(kafka.Text), clientNetworks = Split(clients.Text), appPort = (int)appPort.Value, identityPort = (int)identityPort.Value,
            databasePort = (int)databasePort.Value, password = action.SelectedIndex == 0 ? password.Text : "", accepted = true, language = language.Persian ? "fa" : "en" };
        password.Clear();
        try
        {
            var progress = new Progress<string>(stage => status.Text = language[stage]);
            var cache = await Task.Run(() => SetupMedia.Prepare(stage => ((IProgress<string>)progress).Report(stage)));
            var info = new ProcessStartInfo(Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), @"WindowsPowerShell\v1.0\powershell.exe"))
            { UseShellExecute = false, CreateNoWindow = true, RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
                StandardOutputEncoding = System.Text.Encoding.UTF8, StandardErrorEncoding = System.Text.Encoding.UTF8, WorkingDirectory = cache };
            foreach (var arg in new[] { "-NoLogo", "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-File", Path.Combine(cache, "Setup-Worker.ps1") }) info.ArgumentList.Add(arg);
            using var process = Process.Start(info) ?? throw new IOException("Worker did not start.");
            // Drain without retaining native/PowerShell error output. It may contain secrets.
            var errors = Task.Run(async () => { var buffer = new char[4096]; while (await process.StandardError.ReadAsync(buffer) > 0) { } });
            await process.StandardInput.WriteLineAsync(JsonSerializer.Serialize(request)); request = null; process.StandardInput.Close();
            while (await process.StandardOutput.ReadLineAsync() is string line)
            {
                if (line.Length > 16384) continue;
                try
                {
                    using var item = JsonDocument.Parse(line); var data = item.RootElement;
                    var stage = data.GetProperty("stage").GetString();
                    if (stage is "running" or "verified") status.Text = language[stage];
                    if (stage is "complete" or "removed" or "reboot" or "recovered" or "failed")
                    {
                        outcome = stage;
                        if (data.TryGetProperty("root", out var r)) resultRoot = r.GetString() ?? "";
                        if (stage == "complete")
                        {
                            origin = data.GetProperty("origin").GetString()!; firstAdmin = data.GetProperty("firstAdmin").GetBoolean();
                            if (data.TryGetProperty("code", out var c)) code = c.GetString() ?? "";
                        }
                    }
                }
                catch (Exception e) when (e is JsonException or KeyNotFoundException or InvalidOperationException) { }
            }
            await process.WaitForExitAsync(); await errors;
            if (process.ExitCode != 0 && process.ExitCode != 3010) outcome = "failed";
        }
        catch (Exception e) when (e is IOException or InvalidDataException or UnauthorizedAccessException or System.ComponentModel.Win32Exception) { outcome = "failed"; }
        finally { busy = false; page = 5; Render(); }
    }

    private static void Open(string url)
    {
        try { Process.Start(new ProcessStartInfo(url) { UseShellExecute = true }); }
        catch (System.ComponentModel.Win32Exception) { /* URL remains visible and can be copied. */ }
    }
}
