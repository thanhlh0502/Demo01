using MasterDataMigration.Models;
using MasterDataMigration.Services;
using Microsoft.Extensions.Configuration;
using Serilog;

// Configure Serilog
Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .WriteTo.Console()
    .WriteTo.File("logs/migration-.log", rollingInterval: RollingInterval.Day)
    .CreateLogger();

try
{
    Log.Information("Master Data Migration Tool starting...");

    // Support custom config file via command-line: dotnet run -- appsettings.Test.json
    var configFile = args.Length > 0 ? args[0] : "appsettings.json";
    Log.Information("Using config file: {ConfigFile}", configFile);

    // Load configuration
    var configuration = new ConfigurationBuilder()
        .SetBasePath(AppContext.BaseDirectory)
        .AddJsonFile(configFile, optional: false)
        .Build();

    var settings = new MigrationSettings();
    configuration.GetSection("MigrationSettings").Bind(settings);

    // Validate
    if (string.IsNullOrEmpty(settings.SourceConnectionString))
        throw new InvalidOperationException("SourceConnectionString is required.");
    if (string.IsNullOrEmpty(settings.TargetConnectionString))
        throw new InvalidOperationException("TargetConnectionString is required.");
    if (string.IsNullOrEmpty(settings.StagingConnectionString))
        throw new InvalidOperationException("StagingConnectionString is required.");
    if (settings.TradingPartners.Count == 0)
        throw new InvalidOperationException("At least one TradingPartner is required.");
    if (settings.Tables.Count == 0)
        throw new InvalidOperationException("At least one Table must be configured.");

    // Run migration
    var runner = new MigrationRunner(settings, Log.Logger);
    var results = await runner.RunAsync();

    // Summary
    Log.Information("========== SYNC SUMMARY ==========");
    foreach (var r in results)
    {
        var status = r.Success ? "OK" : "FAIL";
        Log.Information("[{Status}] {Table}: Extracted={Extracted}, Inserted={Inserted}, Updated={Updated}, Deleted={Deleted}, Duration={Duration}",
            status, r.TableName, r.ExtractedRows, r.InsertedRows, r.UpdatedRows, r.DeletedRows, r.Duration);
        if (!r.Success)
            Log.Error("  Error: {Error}", r.ErrorMessage);
    }
    Log.Information("==================================");

    return results.All(r => r.Success) ? 0 : 1;
}
catch (Exception ex)
{
    Log.Fatal(ex, "Migration tool terminated unexpectedly.");
    return 1;
}
finally
{
    await Log.CloseAndFlushAsync();
}
