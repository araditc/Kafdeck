using Kafdeck.Infrastructure.Persistence;
using Microsoft.Data.Sqlite;
using Xunit;
namespace Kafdeck.Architecture.Tests;
public sealed class V08W68LeaseMigrationCandidateTests
{
    [Fact] public async Task Sqlite_provider_must_not_silently_install_postgresql_tables()
    {
        await using var db=new SqliteConnection("Data Source=:memory:");
        await db.OpenAsync();
        await using var tx=await db.BeginTransactionAsync();
        await Assert.ThrowsAsync<NotSupportedException>(()=>W68LeaseMigrationCandidate.InstallPostgreSqlAsync(db,tx));
    }
}
