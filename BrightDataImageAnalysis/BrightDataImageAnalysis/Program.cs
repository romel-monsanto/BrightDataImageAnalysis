using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Microsoft.Extensions.Configuration;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

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

var results = new List<ImageAnalysisResult>();

Console.WriteLine($"Scanning folder: {blobConfig.FolderName}");

await foreach (BlobItem blob in containerClient.GetBlobsAsync(prefix: blobConfig.FolderName))
{
    var ext = Path.GetExtension(blob.Name);
    if (!imageExtensions.Contains(ext))
        continue;

    Console.WriteLine($"\n--- Processing: {blob.Name} ---");

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

    // Submit to Content Understanding API
    var submitUrl = $"{cuConfig.Endpoint}/contentunderstanding/analyzers/{cuConfig.AnalyzerId}:analyze" +
                    $"?stringEncoding=utf16&api-version={cuConfig.ApiVersion}&processingLocation=global";

    var requestBody = JsonSerializer.Serialize(new { url = sasUri.ToString() });
    var httpContent = new StringContent(requestBody, Encoding.UTF8, "application/json");

    var submitResponse = await httpClient.PostAsync(submitUrl, httpContent);
    var submitBody = await submitResponse.Content.ReadAsStringAsync();

    if (!submitResponse.IsSuccessStatusCode)
    {
        Console.WriteLine($"  Submit failed ({submitResponse.StatusCode}): {submitBody}");
        continue;
    }

    var submitJson = JsonNode.Parse(submitBody);
    var operationId = submitJson?["id"]?.GetValue<string>();

    if (string.IsNullOrEmpty(operationId))
    {
        Console.WriteLine($"  No operation id in response: {submitBody}");
        continue;
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

        Console.WriteLine($"  Status: {status}");

        if (status == "Succeeded")
        {
            resultJson = pollBody;
            break;
        }
        else if (status == "Failed")
        {
            Console.WriteLine($"  Analysis failed: {pollBody}");
            break;
        }
    }

    if (resultJson is null)
    {
        Console.WriteLine("  Timed out or failed — skipping.");
        continue;
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

    results.Add(new ImageAnalysisResult(
        blob.Name,
        contentSummary,
        interestPaths,
        traitPaths,
        interestParent,
        interestChildren,
        interestGrandChildren,
        traitParent,
        traitChildren,
        traitGrandChildren));
}

static (string parent, string children, string grandChildren) ParseHierarchy(List<string> paths)
{
    if (paths.Count == 0)
        return (string.Empty, string.Empty, string.Empty);

    var split = paths.Select(p => p.Split(" > ")).ToList();

    var parent = split.Select(s => s[0]).FirstOrDefault() ?? string.Empty;

    var children = string.Join(" | ",
        split.Where(s => s.Length >= 2).Select(s => s[1]).Distinct());

    var grandChildren = string.Join(" | ",
        split.Where(s => s.Length >= 3).Select(s => s[2]).Distinct());

    return (parent, children, grandChildren);
}

Console.WriteLine("\n=== Results ===");
Console.WriteLine(JsonSerializer.Serialize(results, new JsonSerializerOptions { WriteIndented = true }));
Console.WriteLine($"\nTotal processed: {results.Count}");

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
    string ApiVersion);

record BlobStorageConfig(
    string ConnectionString,
    string ContainerName,
    string BaseUrl,
    string FolderName);
