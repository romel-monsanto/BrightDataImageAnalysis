using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

var stopwatch = System.Diagnostics.Stopwatch.StartNew();

var config = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false)
    .Build();

var cuConfig = config.GetSection("ContentUnderstanding").Get<ContentUnderstandingConfig>();
var blobConfig = config.GetSection("AzureBlobStorage").Get<BlobStorageConfig>();

ArgumentNullException.ThrowIfNull(cuConfig, "ContentUnderstanding config missing");
ArgumentNullException.ThrowIfNull(blobConfig, "AzureBlobStorage config missing");

var containerClient = new BlobServiceClient(blobConfig.ConnectionString)
    .GetBlobContainerClient(blobConfig.ContainerName);

using var httpClient = new HttpClient();
httpClient.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", cuConfig.SubscriptionKey);

var imageExtensions = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
{
    ".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff"
};

// CSV output — open in append mode to support resuming a previous run
var csvPath = Path.Combine(AppContext.BaseDirectory, "results.csv");
bool resuming = File.Exists(csvPath);

var completed = new HashSet<string>();
if (resuming)
{
    foreach (var line in File.ReadLines(csvPath).Skip(1)) // skip header
    {
        // FileName is the first column; blob names don't contain commas so split is safe
        var fileName = line.Split(',')[0].Trim('"');
        if (!string.IsNullOrEmpty(fileName))
            completed.Add(fileName);
    }
    Console.WriteLine($"Resuming: {completed.Count} images already processed, skipping them.");
}

using var csvWriter = new StreamWriter(csvPath, append: resuming);
if (!resuming)
    csvWriter.WriteLine("FileName,ContentSummary,InterestHierarchyPath,InterestParent," +
                        "InterestChildren,InterestGrandChildren,TraitHierarchyPath," +
                        "TraitParent,TraitChildren,TraitGrandChildren");

var csvLock = new object();

// Phase 1: collect all image blobs not yet processed
Console.WriteLine($"Scanning folder: {blobConfig.FolderName}");
var imageBlobs = new List<BlobItem>();
await foreach (BlobItem blob in containerClient.GetBlobsAsync(prefix: blobConfig.FolderName))
{
    var ext = Path.GetExtension(blob.Name);
    if (imageExtensions.Contains(ext) && !completed.Contains(blob.Name))
        imageBlobs.Add(blob);
}

int processedCount = 0;
int totalCount = imageBlobs.Count;
Console.WriteLine($"Found {totalCount} images to process. Concurrency: {cuConfig.MaxConcurrency}");

// Phase 2: analyze all images in parallel
await Parallel.ForEachAsync(imageBlobs,
    new ParallelOptions { MaxDegreeOfParallelism = cuConfig.MaxConcurrency },
    async (blob, _) =>
    {
        var result = await AnalyzeImageAsync(blob);
        var n = Interlocked.Increment(ref processedCount);

        if (result is not null)
        {
            lock (csvLock)
            {
                csvWriter.WriteLine(ToCsvRow(result));
                csvWriter.Flush();
            }
        }

        Console.WriteLine($"[{n}/{totalCount}] {(result is not null ? "Done" : "Skipped")}: {blob.Name}");
    });

stopwatch.Stop();
Console.WriteLine($"\nDone. Results saved to: {csvPath}");
Console.WriteLine($"Total execution time: {stopwatch.Elapsed:hh\\:mm\\:ss\\.fff}");

async Task<ImageAnalysisResult?> AnalyzeImageAsync(BlobItem blob)
{
    // Generate blob-level SAS URI (1 hour, read-only)
    var blobClient = containerClient.GetBlobClient(blob.Name);
    var sasBuilder = new BlobSasBuilder
    {
        BlobContainerName = blobConfig.ContainerName,
        BlobName = blob.Name,
        Resource = "b",
        ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
    };
    sasBuilder.SetPermissions(BlobSasPermissions.Read);
    var sasUri = blobClient.GenerateSasUri(sasBuilder);

    var submitUrl = $"{cuConfig.Endpoint}/contentunderstanding/analyzers/{cuConfig.AnalyzerId}:analyze" +
                    $"?stringEncoding=utf16&api-version={cuConfig.ApiVersion}&processingLocation=global";

    var requestBody = JsonSerializer.Serialize(new { url = sasUri.ToString() });

    // Submit with retry on 429
    HttpResponseMessage submitResponse = null!;
    string submitBody = string.Empty;

    for (int retry = 0; retry <= 3; retry++)
    {
        var httpContent = new StringContent(requestBody, Encoding.UTF8, "application/json");
        submitResponse = await httpClient.PostAsync(submitUrl, httpContent);
        submitBody = await submitResponse.Content.ReadAsStringAsync();

        if ((int)submitResponse.StatusCode == 429)
        {
            var wait = submitResponse.Headers.RetryAfter?.Delta ?? TimeSpan.FromSeconds(5);
            Console.WriteLine($"  Rate limited [{blob.Name}], retrying in {wait.TotalSeconds}s");
            await Task.Delay(wait);
            continue;
        }
        break;
    }

    if (!submitResponse.IsSuccessStatusCode)
    {
        Console.WriteLine($"  Submit failed ({submitResponse.StatusCode}) [{blob.Name}]: {submitBody}");
        return null;
    }

    var submitJson = JsonNode.Parse(submitBody);
    var operationId = submitJson?["id"]?.GetValue<string>();

    if (string.IsNullOrEmpty(operationId))
    {
        Console.WriteLine($"  No operation id [{blob.Name}]: {submitBody}");
        return null;
    }

    var pollUrl = $"{cuConfig.Endpoint}/contentunderstanding/analyzerResults/{operationId}" +
                  $"?api-version={cuConfig.ApiVersion}";

    // Poll until complete (max 30 attempts × 2s = 60s)
    string? resultJson = null;
    for (int attempt = 0; attempt < 30; attempt++)
    {
        await Task.Delay(TimeSpan.FromSeconds(2));

        var pollResponse = await httpClient.GetAsync(pollUrl);
        var pollBody = await pollResponse.Content.ReadAsStringAsync();
        var pollJson = JsonNode.Parse(pollBody);
        var status = pollJson?["status"]?.GetValue<string>();

        if (status == "Succeeded")
        {
            resultJson = pollBody;
            break;
        }
        else if (status == "Failed")
        {
            Console.WriteLine($"  Analysis failed [{blob.Name}]: {pollBody}");
            break;
        }
    }

    if (resultJson is null)
    {
        Console.WriteLine($"  Timed out or failed — skipping: {blob.Name}");
        return null;
    }

    // Map to ImageAnalysisResult
    var node = JsonNode.Parse(resultJson);
    var fields = node?["result"]?["contents"]?[0]?["fields"];

    var contentSummary = fields?["contentSummary"]?["valueString"]?.GetValue<string>();

    var interestPaths = fields?["interestHierarchyPath"]?["valueArray"]
        ?.AsArray()
        .Select(x => x?["valueString"]?.GetValue<string>() ?? string.Empty)
        .ToList() ?? [];

    var traitPaths = fields?["traitHierarchyPath"]?["valueArray"]
        ?.AsArray()
        .Select(x => x?["valueString"]?.GetValue<string>() ?? string.Empty)
        .ToList() ?? [];

    var (interestParent, interestChildren, interestGrandChildren) = ParseHierarchy(interestPaths);
    var (traitParent, traitChildren, traitGrandChildren) = ParseHierarchy(traitPaths);

    return new ImageAnalysisResult(
        blob.Name,
        contentSummary,
        interestPaths,
        traitPaths,
        interestParent,
        interestChildren,
        interestGrandChildren,
        traitParent,
        traitChildren,
        traitGrandChildren);
}

static string ToCsvRow(ImageAnalysisResult r) => string.Join(",",
    Csv(r.FileName),
    Csv(r.ContentSummary),
    Csv(string.Join(" | ", r.InterestHierarchyPath)),
    Csv(r.InterestParent),
    Csv(r.InterestChildren),
    Csv(r.InterestGrandChildren),
    Csv(string.Join(" | ", r.TraitHierarchyPath)),
    Csv(r.TraitParent),
    Csv(r.TraitChildren),
    Csv(r.TraitGrandChildren));

static string Csv(string? value)
{
    if (string.IsNullOrEmpty(value)) return string.Empty;
    if (value.Contains(',') || value.Contains('"') || value.Contains('\n'))
        return $"\"{value.Replace("\"", "\"\"")}\"";
    return value;
}

static (string parent, string children, string grandChildren) ParseHierarchy(List<string> paths)
{
    if (paths.Count == 0)
        return (string.Empty, string.Empty, string.Empty);

    var split = paths.Select(p => p.Split(" > ")).ToList();

    var parent = string.Join(" | ",
        split.Where(s => s.Length >= 1).Select(s => s[0]).Distinct());

    var children = string.Join(" | ",
        split.Where(s => s.Length >= 2).Select(s => s[1]).Distinct());

    var grandChildren = string.Join(" | ",
        split.Where(s => s.Length >= 3).Select(s => s[2]).Distinct());

    return (parent, children, grandChildren);
}

record ImageAnalysisResult(
    string FileName,
    string? ContentSummary,
    List<string> InterestHierarchyPath,
    List<string> TraitHierarchyPath,
    string InterestParent,
    string InterestChildren,
    string InterestGrandChildren,
    string TraitParent,
    string TraitChildren,
    string TraitGrandChildren);

record ContentUnderstandingConfig(
    string Endpoint,
    string SubscriptionKey,
    string AnalyzerId,
    string ApiVersion,
    int MaxConcurrency);

record BlobStorageConfig(
    string ConnectionString,
    string ContainerName,
    string BaseUrl,
    string FolderName);
