// Copyright (c) Microsoft. All rights reserved.

using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace Microsoft.Agents.AI.Hosting.AzureFunctions.IntegrationTests;

public sealed partial class SampleSettingsTemplateValidation
{
    private const string TemplateFileName = "local.settings.json.template";

    // Path.GetFullPath does not collapse ".." segments when the base path carries the Windows
    // "\\?\" extended-length prefix (e.g. deeply nested worktrees), so walk up via DirectoryInfo.Parent
    // instead, which operates on the already-resolved path components.
    private static readonly string s_samplesPath = FindSamplesPath();

    private static string FindSamplesPath()
    {
        for (DirectoryInfo? directory = new(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            string candidate = Path.Combine(directory.FullName, "samples");
            if (Directory.Exists(Path.Combine(candidate, "DurableAgents", "AzureFunctions")) &&
                Directory.Exists(Path.Combine(candidate, "DurableWorkflows", "AzureFunctions")))
            {
                return candidate;
            }
        }

        throw new DirectoryNotFoundException(
            $"Could not locate the 'dotnet/samples' directory by walking up from '{AppContext.BaseDirectory}'.");
    }

    [Fact]
    public void TemplatesMatchSampleRequirements()
    {
        AssertLocalSettingsBuildDefaults();

        string[] sampleDirectories =
        [
            .. Directory.GetDirectories(Path.Combine(s_samplesPath, "DurableAgents", "AzureFunctions")),
            .. Directory.GetDirectories(Path.Combine(s_samplesPath, "DurableWorkflows", "AzureFunctions")),
        ];
        foreach (string sampleDirectory in sampleDirectories)
        {
            string templatePath = Path.Combine(sampleDirectory, TemplateFileName);
            Assert.True(File.Exists(templatePath), $"Missing {TemplateFileName} in {sampleDirectory}.");
            AssertAzureFunctionsProject(sampleDirectory);

            using JsonDocument template = JsonDocument.Parse(File.ReadAllText(templatePath));
            JsonElement root = template.RootElement;
            Assert.False(root.GetProperty("IsEncrypted").GetBoolean());

            JsonElement values = root.GetProperty("Values");
            Assert.Equal(JsonValueKind.Object, values.ValueKind);

            HashSet<string> actualKeys = values.EnumerateObject()
                .Select(property => property.Name)
                .ToHashSet(StringComparer.Ordinal);

            HashSet<string> expectedKeys =
            [
                "FUNCTIONS_WORKER_RUNTIME",
                "AzureWebJobsStorage",
                .. GetEnvironmentVariableNames(sampleDirectory),
                .. GetHostConnectionSettingNames(sampleDirectory),
            ];

            Assert.True(
                actualKeys.SetEquals(expectedKeys),
                $"{templatePath} settings mismatch. Expected: {string.Join(", ", expectedKeys.Order())}. " +
                $"Actual: {string.Join(", ", actualKeys.Order())}.");
            Assert.DoesNotContain("TASKHUB_NAME", actualKeys);
            Assert.All(values.EnumerateObject(), property => Assert.False(string.IsNullOrWhiteSpace(property.Value.GetString())));
            Assert.Equal("dotnet-isolated", values.GetProperty("FUNCTIONS_WORKER_RUNTIME").GetString());
            Assert.Equal("UseDevelopmentStorage=true", values.GetProperty("AzureWebJobsStorage").GetString());

            if (values.TryGetProperty("FOUNDRY_PROJECT_ENDPOINT", out JsonElement endpoint))
            {
                Assert.True(
                    Uri.TryCreate(endpoint.GetString(), UriKind.Absolute, out _),
                    $"{templatePath} must provide a syntactically valid placeholder endpoint.");
            }
        }
    }

    private static void AssertLocalSettingsBuildDefaults()
    {
        XDocument buildTargets = XDocument.Load(Path.Combine(s_samplesPath, "Directory.Build.props"));
        XElement localSettingsItem = Assert.Single(
            buildTargets.Descendants("None"),
            item => string.Equals((string?)item.Attribute("Update"), "local.settings.json", StringComparison.Ordinal));

        Assert.Equal("PreserveNewest", localSettingsItem.Element("CopyToOutputDirectory")?.Value);
        Assert.Equal("Never", localSettingsItem.Element("CopyToPublishDirectory")?.Value);
    }

    private static void AssertAzureFunctionsProject(string sampleDirectory)
    {
        string projectPath = Assert.Single(Directory.GetFiles(sampleDirectory, "*.csproj"));
        XDocument project = XDocument.Load(projectPath);
        Assert.Contains(project.Descendants("AzureFunctionsVersion"), property => property.Value == "v4");
    }

    private static IEnumerable<string> GetEnvironmentVariableNames(string sampleDirectory)
    {
        IEnumerable<string> sourcePaths = Directory
            .EnumerateFiles(sampleDirectory, "*.cs", SearchOption.AllDirectories)
            .Where(path => !Path.GetRelativePath(sampleDirectory, path)
                .Split(Path.DirectorySeparatorChar)
                .Any(segment => segment is "bin" or "obj"));

        foreach (string sourcePath in sourcePaths)
        {
            string source = File.ReadAllText(sourcePath);
            foreach (Match match in EnvironmentVariablePattern().Matches(source))
            {
                yield return match.Groups["name"].Value;
            }
        }
    }

    private static IEnumerable<string> GetHostConnectionSettingNames(string sampleDirectory)
    {
        string hostPath = Path.Combine(sampleDirectory, "host.json");
        using JsonDocument host = JsonDocument.Parse(File.ReadAllText(hostPath));

        JsonElement durableTask = host.RootElement
            .GetProperty("extensions")
            .GetProperty("durableTask");

        yield return durableTask
            .GetProperty("storageProvider")
            .GetProperty("connectionStringName")
            .GetString()!;
    }

    [GeneratedRegex("""Environment\.GetEnvironmentVariable\("(?<name>[A-Za-z0-9_]+)"\)""")]
    private static partial Regex EnvironmentVariablePattern();
}
