// Copyright (c) Microsoft. All rights reserved.

using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Microsoft.Agents.AI;
using Microsoft.Extensions.AI;

namespace CustomHistoryProvider;

/// <summary>
/// Stores a text-only chat transcript in sample-local JSON files.
/// </summary>
public sealed class JsonFileChatHistoryProvider : ChatHistoryProvider, IDisposable
{
    public const string ProviderKey = "sample-json-file-history.v1";

    public const string StateKey = "sample-json-history";

    private const int MaxSeedMessageUtf8Bytes = 8 * 1024;

    private static readonly JsonSerializerOptions s_jsonOptions = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = true,
    };

    private readonly string _storeDirectory;
    private readonly int _maxModelMessages;
    private readonly int _maxModelTextUtf8Bytes;
    private readonly SemaphoreSlim _gate = new(1, 1);
    private readonly ConcurrentDictionary<string, byte> _observedHistoryIds = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ModelWindowStatistics> _lastModelWindows = new(StringComparer.Ordinal);

    public JsonFileChatHistoryProvider(
        string storeDirectory,
        int maxModelMessages = 12,
        int maxModelTextUtf8Bytes = 32 * 1024)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(storeDirectory);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxModelMessages);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxModelTextUtf8Bytes);

        this._storeDirectory = Path.GetFullPath(storeDirectory);
        this._maxModelMessages = maxModelMessages;
        this._maxModelTextUtf8Bytes = maxModelTextUtf8Bytes;
    }

    public override IReadOnlyList<string> StateKeys => [StateKey];

    public IReadOnlyList<string> GetObservedHistoryIds() =>
        this._observedHistoryIds.Keys.Order(StringComparer.Ordinal).ToArray();

    public string GetHistoryId(AgentSession session)
    {
        ArgumentNullException.ThrowIfNull(session);
        return this.GetOrCreateReference(session).FileName;
    }

    public async Task<IReadOnlyList<ChatMessage>> ReadMessagesAsync(
        AgentSession session,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(session);
        HistoryReference reference = this.GetOrCreateReference(session);

        await this._gate.WaitAsync(cancellationToken);
        try
        {
            List<StoredChatMessage> stored =
                await this.ReadStoredMessagesAsync(reference, cancellationToken);
            return stored.ConvertAll(ToChatMessage);
        }
        finally
        {
            this._gate.Release();
        }
    }

    public async Task<SeedHistoryResult> SeedHistoryAsync(
        string historyId,
        long minimumStoreBytes,
        int messageTextUtf8Bytes,
        CancellationToken cancellationToken = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(minimumStoreBytes);
        if (messageTextUtf8Bytes is < 256 or > MaxSeedMessageUtf8Bytes)
        {
            throw new ArgumentOutOfRangeException(
                nameof(messageTextUtf8Bytes),
                $"Seed messages must be between 256 and {MaxSeedMessageUtf8Bytes} UTF-8 bytes.");
        }

        HistoryReference reference = this.GetReference(historyId);

        await this._gate.WaitAsync(cancellationToken);
        try
        {
            List<StoredChatMessage> stored =
                await this.ReadStoredMessagesAsync(reference, cancellationToken);
            int initialCount = stored.Count;
            int seedIndex = initialCount;
            long currentBytes = this.GetPersistedBytes(reference);

            while (currentBytes <= minimumStoreBytes)
            {
                int remainingMessages = Math.Max(
                    1,
                    checked((int)Math.Ceiling(
                        (minimumStoreBytes - currentBytes + 1d) / messageTextUtf8Bytes)));

                for (int index = 0; index < remainingMessages; index++)
                {
                    ChatRole role = seedIndex % 2 == 0 ? ChatRole.User : ChatRole.Assistant;
                    stored.Add(CreateSeedMessage(seedIndex++, role, messageTextUtf8Bytes));
                }

                await this.WriteStoredMessagesAsync(reference, stored, cancellationToken);
                currentBytes = this.GetPersistedBytes(reference);
            }

            return new SeedHistoryResult(
                SeededMessageCount: stored.Count - initialCount,
                PersistedMessageCount: stored.Count,
                PersistedBytes: currentBytes,
                MaximumSeedMessageTextUtf8Bytes: messageTextUtf8Bytes);
        }
        finally
        {
            this._gate.Release();
        }
    }

    public async Task<HistoryStatistics> GetStatisticsAsync(
        string historyId,
        CancellationToken cancellationToken = default)
    {
        HistoryReference reference = this.GetReference(historyId);

        await this._gate.WaitAsync(cancellationToken);
        try
        {
            List<StoredChatMessage> stored =
                await this.ReadStoredMessagesAsync(reference, cancellationToken);
            this._lastModelWindows.TryGetValue(historyId, out ModelWindowStatistics? window);

            return new HistoryStatistics(
                HistoryId: historyId,
                PersistedMessageCount: stored.Count,
                PersistedBytes: this.GetPersistedBytes(reference),
                LastModelWindow: window);
        }
        finally
        {
            this._gate.Release();
        }
    }

    protected override async ValueTask<IEnumerable<ChatMessage>> ProvideChatHistoryAsync(
        InvokingContext context,
        CancellationToken cancellationToken = default)
    {
        AgentSession session = context.Session ??
            throw new InvalidOperationException("A session is required for file-backed history.");
        HistoryReference reference = this.GetOrCreateReference(session);

        await this._gate.WaitAsync(cancellationToken);
        try
        {
            List<StoredChatMessage> stored =
                await this.ReadStoredMessagesAsync(reference, cancellationToken);
            List<StoredChatMessage> modelWindow = this.SelectModelWindow(stored);
            this._lastModelWindows[reference.FileName] = new ModelWindowStatistics(
                modelWindow.Count,
                modelWindow.Sum(GetTextUtf8Bytes));
            return modelWindow.ConvertAll(ToChatMessage);
        }
        finally
        {
            this._gate.Release();
        }
    }

    protected override async ValueTask StoreChatHistoryAsync(
        InvokedContext context,
        CancellationToken cancellationToken = default)
    {
        AgentSession session = context.Session ??
            throw new InvalidOperationException("A session is required for file-backed history.");
        HistoryReference reference = this.GetOrCreateReference(session);

        await this._gate.WaitAsync(cancellationToken);
        try
        {
            List<StoredChatMessage> stored =
                await this.ReadStoredMessagesAsync(reference, cancellationToken);
            stored.AddRange(context.RequestMessages.Select(ToStoredMessage));
            stored.AddRange((context.ResponseMessages ?? []).Select(ToStoredMessage));

            await this.WriteStoredMessagesAsync(reference, stored, cancellationToken);
        }
        finally
        {
            this._gate.Release();
        }
    }

    private HistoryReference GetOrCreateReference(AgentSession session)
    {
        HistoryReference? reference = session.StateBag.GetValue<HistoryReference>(StateKey);
        if (reference is null)
        {
            reference = new HistoryReference
            {
                FileName = $"{Guid.NewGuid():N}.json",
            };
            session.StateBag.SetValue(StateKey, reference);
        }

        this.ValidateHistoryId(reference.FileName);
        this._observedHistoryIds.TryAdd(reference.FileName, 0);
        return reference;
    }

    private static StoredChatMessage ToStoredMessage(ChatMessage message)
    {
        if (message.Contents.Any(content => content is not TextContent))
        {
            throw new NotSupportedException(
                "This sample provider intentionally supports text content only. " +
                "A production provider must serialize every content type used by the agent.");
        }

        return new StoredChatMessage
        {
            Role = message.Role.Value,
            Text = message.Text,
            MessageId = message.MessageId,
            CreatedAt = message.CreatedAt,
        };
    }

    private static ChatMessage ToChatMessage(StoredChatMessage message) =>
        new(ToChatRole(message.Role), message.Text)
        {
            MessageId = message.MessageId,
            CreatedAt = message.CreatedAt,
        };

    private static ChatRole ToChatRole(string role) =>
        role switch
        {
            "assistant" => ChatRole.Assistant,
            "system" => ChatRole.System,
            "tool" => ChatRole.Tool,
            "user" => ChatRole.User,
            _ => throw new JsonException($"Unsupported chat role '{role}'."),
        };

    private static StoredChatMessage CreateSeedMessage(
        int index,
        ChatRole role,
        int messageTextUtf8Bytes)
    {
        string prefix = $"[SIMULATED OLD HISTORY {index:D6} {role.Value}] ";
        if (Encoding.UTF8.GetByteCount(prefix) > messageTextUtf8Bytes)
        {
            throw new ArgumentOutOfRangeException(nameof(messageTextUtf8Bytes));
        }

        string text = prefix + new string(
            (char)('a' + (index % 26)),
            messageTextUtf8Bytes - prefix.Length);
        return new StoredChatMessage
        {
            Role = role.Value,
            Text = text,
            MessageId = $"simulated-old-history-{index:D6}",
            CreatedAt = DateTimeOffset.UnixEpoch.AddSeconds(index),
        };
    }

    private static int GetTextUtf8Bytes(StoredChatMessage message) =>
        Encoding.UTF8.GetByteCount(message.Text ?? string.Empty);

    private List<StoredChatMessage> SelectModelWindow(IReadOnlyList<StoredChatMessage> stored)
    {
        List<StoredChatMessage> newestFirst = [];
        int textBytes = 0;

        for (int index = stored.Count - 1;
             index >= 0 && newestFirst.Count < this._maxModelMessages;
             index--)
        {
            int messageBytes = GetTextUtf8Bytes(stored[index]);
            if (textBytes + messageBytes > this._maxModelTextUtf8Bytes)
            {
                break;
            }

            newestFirst.Add(stored[index]);
            textBytes += messageBytes;
        }

        newestFirst.Reverse();
        return newestFirst;
    }

    private async Task<List<StoredChatMessage>> ReadStoredMessagesAsync(
        HistoryReference reference,
        CancellationToken cancellationToken)
    {
        string path = this.GetHistoryPath(reference);
        if (!File.Exists(path))
        {
            return [];
        }

        await using FileStream stream = File.OpenRead(path);
        return await JsonSerializer.DeserializeAsync<List<StoredChatMessage>>(
                stream,
                s_jsonOptions,
                cancellationToken)
            ?? [];
    }

    private async Task WriteStoredMessagesAsync(
        HistoryReference reference,
        List<StoredChatMessage> messages,
        CancellationToken cancellationToken)
    {
        Directory.CreateDirectory(this._storeDirectory);
        string path = this.GetHistoryPath(reference);
        string temporaryPath = Path.Combine(
            this._storeDirectory,
            $"{reference.FileName}.{Guid.NewGuid():N}.writing");

        try
        {
            await using (FileStream stream = new(
                temporaryPath,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.None,
                bufferSize: 4096,
                FileOptions.Asynchronous | FileOptions.WriteThrough))
            {
                await JsonSerializer.SerializeAsync(
                    stream,
                    messages,
                    s_jsonOptions,
                    cancellationToken);
                await stream.FlushAsync(cancellationToken);
            }

            File.Move(temporaryPath, path, overwrite: true);
        }
        finally
        {
            if (File.Exists(temporaryPath))
            {
                File.Delete(temporaryPath);
            }
        }
    }

    private HistoryReference GetReference(string historyId)
    {
        this.ValidateHistoryId(historyId);
        this._observedHistoryIds.TryAdd(historyId, 0);
        return new HistoryReference { FileName = historyId };
    }

    private void ValidateHistoryId(string historyId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(historyId);
        if (!string.Equals(Path.GetFileName(historyId), historyId, StringComparison.Ordinal) ||
            !historyId.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            throw new ArgumentException("History IDs must be JSON file names.", nameof(historyId));
        }
    }

    private long GetPersistedBytes(HistoryReference reference)
    {
        string path = this.GetHistoryPath(reference);
        return File.Exists(path) ? new FileInfo(path).Length : 0;
    }

    private string GetHistoryPath(HistoryReference reference) =>
        Path.Combine(this._storeDirectory, Path.GetFileName(reference.FileName));

    public void Dispose() => this._gate.Dispose();

    public sealed class HistoryReference
    {
        public string FileName { get; set; } = string.Empty;
    }

    public sealed record SeedHistoryResult(
        int SeededMessageCount,
        int PersistedMessageCount,
        long PersistedBytes,
        int MaximumSeedMessageTextUtf8Bytes);

    public sealed record ModelWindowStatistics(int MessageCount, int TextUtf8Bytes);

    public sealed record HistoryStatistics(
        string HistoryId,
        int PersistedMessageCount,
        long PersistedBytes,
        ModelWindowStatistics? LastModelWindow);

    private sealed class StoredChatMessage
    {
        public string Role { get; set; } = string.Empty;

        public string? Text { get; set; }

        public string? MessageId { get; set; }

        public DateTimeOffset? CreatedAt { get; set; }
    }
}
