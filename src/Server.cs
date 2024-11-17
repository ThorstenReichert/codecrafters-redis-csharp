using codecrafters_redis.src;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Immutable;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
using TcpListener server = new(IPAddress.Any, 6379);
server.Start();

var clientCounter = 0;
var keyValueStore = new ConcurrentDictionary<string, RespObject>();

while (true)
{
    var handler = await server.AcceptTcpClientAsync().ConfigureAwait(false);

    _ = new RedisClientHandler(handler.GetStream(), keyValueStore, clientCounter++).RunAsync();
}

internal sealed class RedisClientHandler(Stream redisClientStream, ConcurrentDictionary<string, RespObject> keyValueStore, int id)
{
    private static readonly byte[] CmdPing = Encoding.ASCII.GetBytes("PING");
    private static readonly byte[] CmdEcho = Encoding.ASCII.GetBytes("ECHO");
    private static readonly byte[] CmdSet = Encoding.ASCII.GetBytes("SET");
    private static readonly byte[] CmdGet = Encoding.ASCII.GetBytes("GET");

    private void LogIncoming(RespObject? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogIncoming(string? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogOutgoing(RespObject? message) => Console.WriteLine($"{id,3} << {message}");
    private void LogError(Exception? error) => Console.WriteLine($"{id,3} !! {error}");
    private void LogError(string? error) => Console.WriteLine($"{id,3} !! {error}");

    public async Task RunAsync()
    {
        try
        {
            await using var stream = redisClientStream;
            var reader = PipeReader.Create(stream);

            while (true)
            {
                var command = await NextRespObjectAsync(reader).ConfigureAwait(false);

                LogIncoming(command);

                if (command is RespArray respArray)
                {
                    if (respArray.Items is [RespBulkString pingCmd]
                        && pingCmd.Value.AsSpan().SequenceEqual(CmdPing))
                    {
                        var response = new RespSimpleString { Value = "PONG" };

                        LogOutgoing(response);

                        await response.WriteToAsync(stream).ConfigureAwait(false);
                        await stream.FlushAsync().ConfigureAwait(false);
                    }

                    else if (respArray.Items is [RespBulkString echoCmd, RespBulkString echoArg]
                        && echoCmd.Value.AsSpan().SequenceEqual(CmdEcho))
                    {
                        var response = echoArg;

                        LogOutgoing(response);

                        await response.WriteToAsync(stream).ConfigureAwait(false);
                        await stream.FlushAsync().ConfigureAwait(false);
                    }

                    else if (respArray.Items is [RespBulkString setCmd, RespBulkString setName, RespObject setValue]
                        && setCmd.Value.AsSpan().SequenceEqual(CmdSet))
                    {
                        keyValueStore[setName.AsText()] = setValue;

                        var response = new RespSimpleString { Value = "OK" };

                        LogOutgoing(response);

                        await response.WriteToAsync(stream).ConfigureAwait(false);
                        await stream.FlushAsync().ConfigureAwait(false);
                    }

                    else if (respArray.Items is [RespBulkString getCmd, RespBulkString getName]
                        && getCmd.Value.AsSpan().SequenceEqual(CmdGet))
                    {
                        if (keyValueStore.TryGetValue(getName.AsText(), out var foundResponse))
                        {
                            LogOutgoing(foundResponse);

                            await foundResponse.WriteToAsync(stream).ConfigureAwait(false);
                            await stream.FlushAsync().ConfigureAwait(false);
                        }
                        else
                        {
                            var notFoundResponse = RespNullBulkString.Instance;

                            LogOutgoing(notFoundResponse);

                            await notFoundResponse.WriteToAsync(stream).ConfigureAwait(false);
                            await stream.FlushAsync().ConfigureAwait(false);
                        }
                    }
                }
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            LogError(e);
        }
    }

    async Task<RespObject> NextRespObjectAsync(PipeReader reader)
    {
        while (true)
        {
            var result = await reader.ReadAtLeastAsync(2).ConfigureAwait(false);
            var buffer = result.Buffer;

            LogIncoming($"Read [Buffer = {buffer.ToDebugString()}]");

            var lineEnd = buffer.PositionOf((byte) '\r');
            if (!lineEnd.HasValue)
            {
                reader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            var lineBuffer = buffer.Slice(0, lineEnd.Value);
            var consumed = buffer.GetPosition(2, lineEnd.Value);

            var token = ParseTypeToken(ref buffer);

            if (token == '+')
            {
                LogIncoming("Next simple string");
                reader.AdvanceTo(consumed);

                var simpleStringBuffer = buffer.ToArray();
                var simpleStringValue = Encoding.UTF8.GetString(simpleStringBuffer.AsSpan(1, simpleStringBuffer.Length - 2));

                return new RespSimpleString { Value = simpleStringValue };
            }
            else if (token == '$')
            {
                LogIncoming("Next bulk string");

                var lengthSlice = lineBuffer.Slice(1);
                var length = ParseInteger(ref lengthSlice);
                reader.AdvanceTo(consumed);

                return await NextRespBulkStringAsync(reader, length);
            }
            else if (token == '*')
            {
                LogIncoming("Next array");

                var lengthSlice = lineBuffer.Slice(1);
                var length = ParseInteger(ref lengthSlice);
                var builder = new RespObject[length];

                reader.AdvanceTo(consumed);

                for (var i = 0; i < length; i++)
                {
                    builder[i] = await NextRespObjectAsync(reader);
                }

                return new RespArray { Items = builder.ToImmutableArray() };
            }
            else
            {
                reader.AdvanceTo(consumed);
                throw new NotImplementedException($"Unrecognized object [Bytes = {buffer.ToDebugString()}]");
            }
        }
    }

    byte ParseTypeToken(ref readonly ReadOnlySequence<byte> buffer)
    {
        var reader = new SequenceReader<byte>(buffer);
        if (reader.TryPeek(out var typeToken))
        {
            LogIncoming($"Next type token [Token = {typeToken}]");
            return typeToken;
        }
        else
        {
            throw new InvalidOperationException("Buffer length must be positive.");
        }
    }

    int ParseInteger(ref readonly ReadOnlySequence<byte> buffer)
    {
        // Maximum number of digits of the string encoding the bulk string length.
        const int MaxLength = 10;

        if (buffer.Length > MaxLength)
        {
            throw new InvalidFormatException($"Integer length must not be longer than {MaxLength} digits [Length = {buffer.Length}]");
        }

        Span<byte> lengthSpan = stackalloc byte[MaxLength];
        buffer.CopyTo(lengthSpan);

        var result = 0;
        var factor = 1;
        for (var i = (int) buffer.Length - 1; i >= 0; i--)
        {
            var digit = lengthSpan[i];
            if (!char.IsAsciiDigit((char) digit))
            {
                throw new InvalidFormatException($"BulkString length part contains non-digits [Length = {Encoding.ASCII.GetString(lengthSpan)}]");
            }
            result += factor * (digit - 48);
            factor *= 10;
        }

        LogIncoming($"Next integer [Value = {result}]");

        return result;
    }

    static async Task<RespBulkString> NextRespBulkStringAsync(PipeReader reader, long length)
    {
        if (length < 0)
        {
            throw new InvalidFormatException($"BulkString length must be non-negative [Length = {length}]");
        }

        RespBulkString respBulkString;
        while (true)
        {
            var result = await reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;

            if (buffer.Length < length)
            {
                continue;
            }

            var bulkStringSlice = buffer.Slice(0, length);
            var bulkString = new byte[length];
            bulkStringSlice.CopyTo(bulkString);

            reader.AdvanceTo(buffer.GetPosition(1, bulkStringSlice.End));

            respBulkString = new() { Value = bulkString };
            break;
        }

        while (true)
        {
            var result = await reader.ReadAsync().ConfigureAwait(false);
            var buffer = result.Buffer;

            var lineEnd = buffer.PositionOf((byte) '\n');
            if (!lineEnd.HasValue)
            {
                continue;
            }

            reader.AdvanceTo(buffer.GetPosition(1, lineEnd.Value));

            return respBulkString;
        }
    }
}

internal abstract class RespObject
{
    public abstract override string ToString();

    public abstract ValueTask WriteToAsync(Stream target);
}

internal sealed class RespArray : RespObject
{
    public required ImmutableArray<RespObject> Items { get; init; }

    public override string ToString()
    {
        return $"[{string.Join(", ", Items)}]";
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        var preamble = Encoding.ASCII.GetBytes($"*{Items.Length}\r\n");

        await target.WriteAsync(preamble).ConfigureAwait(false);
        foreach (var item in Items)
        {
            await item.WriteToAsync(target).ConfigureAwait(false);
        }
    }
}

internal sealed class RespBulkString : RespObject
{
    private static readonly byte[] EndOfPart = Encoding.ASCII.GetBytes("\r\n");

    public required byte[] Value { get; init; }

    public string AsText()
    {
        return Encoding.ASCII.GetString(Value);
    }

    public override string ToString()
    {
        return Encoding.ASCII.GetString(Value);
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        var preamble = Encoding.ASCII.GetBytes($"${Value.Length}\r\n");

        await target.WriteAsync(preamble).ConfigureAwait(false);
        await target.WriteAsync(Value).ConfigureAwait(false);
        await target.WriteAsync(EndOfPart).ConfigureAwait(false);
    }
}

internal sealed class RespNullBulkString : RespObject
{
    private static readonly byte[] Payload = Encoding.ASCII.GetBytes("$-1\r\n");

    private RespNullBulkString()
    {
    }

    public static RespNullBulkString Instance { get; } = new();

    public override string ToString()
    {
        return "<null-blk>";
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        await target.WriteAsync(Payload).ConfigureAwait(false);
    }
}

internal sealed class RespSimpleString : RespObject
{
    public required string Value { get; init; }

    public override string ToString()
    {
        return Value;
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        var payload = Encoding.ASCII.GetBytes($"+{Value}\r\n");

        await target.WriteAsync(payload).ConfigureAwait(false);
    }
}

internal sealed class RespNull : RespObject
{
    private static readonly byte[] Payload = Encoding.ASCII.GetBytes("_\r\n");

    private RespNull()
    {
    }

    public static RespNull Instance { get; } = new();

    public override string ToString()
    {
        return "<null>";
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        await target.WriteAsync(Payload).ConfigureAwait(false);
    }
}

public class InvalidFormatException : Exception
{
    public InvalidFormatException()
    {
    }

    public InvalidFormatException(string? message) : base(message)
    {
    }

    public InvalidFormatException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}