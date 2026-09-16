using Kafdeck.Core;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapGet("/healthz", () => Results.Ok(new
{
    status = "ok",
    product = ProductIdentity.Name,
}));

app.Run();

public partial class Program;
