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
var keyValueStore = new ConcurrentDictionary<string, (RespObject Value, DateTime Expiration)>();

while (true)
{
    var handler = await server.AcceptTcpClientAsync().ConfigureAwait(false);

    _ = new RedisClientHandler(handler.GetStream(), keyValueStore, clientCounter++).RunAsync();
}

public sealed class RedisClientHandler(Stream redisClientStream, ConcurrentDictionary<string, (RespObject Value, DateTime Expiration)> keyValueStore, int id)
{
    private static readonly byte[] CmdPing = Encoding.ASCII.GetBytes("PING");
    private static readonly byte[] CmdEcho = Encoding.ASCII.GetBytes("ECHO");
    private static readonly byte[] CmdSet = Encoding.ASCII.GetBytes("SET");
    private static readonly byte[] CmdGet = Encoding.ASCII.GetBytes("GET");

    private static readonly byte[] OptPx = Encoding.ASCII.GetBytes("px");

    private void LogIncoming(RespObject? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogIncoming(string? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogOutgoing(RespObject? message) => Console.WriteLine($"{id,3} << {message}");
    private void LogOutgoing(string? message) => Console.WriteLine($"{id,3} << {message}");
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

                    else if (respArray.Items is [RespBulkString setCmd, RespBulkString setName, RespObject setValue, .. var setOptional]
                        && setCmd.Value.AsSpan().SequenceEqual(CmdSet))
                    {
                        if (setOptional is [RespBulkString pxOption, RespBulkString pxTimeout]
                            && pxOption.Value.AsSpan().SequenceEqual(OptPx))
                        {
                            var expirationDuration = int.Parse(pxTimeout.Value);
                            var expiration = DateTime.UtcNow + TimeSpan.FromMilliseconds(expirationDuration);

                            LogOutgoing($"Expires [UTC = {expiration:u}]");

                            keyValueStore[setName.AsText()] = (setValue, expiration);
                        }
                        else
                        {
                            keyValueStore[setName.AsText()] = (setValue, DateTime.MaxValue);
                        }

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
                            var referenceTimestamp = DateTime.UtcNow;
                            if (foundResponse.Expiration < referenceTimestamp)
                            {
                                LogOutgoing($"Expired [UTC = {foundResponse.Expiration:u}, Now = {referenceTimestamp:u}]");

                                var expiredResponse = RespNullBulkString.Instance;

                                LogOutgoing(expiredResponse);

                                await expiredResponse.WriteToAsync(stream).ConfigureAwait(false);
                                await stream.FlushAsync().ConfigureAwait(false);
                            }
                            else
                            {
                                LogOutgoing($"Not Expired [UTC = {foundResponse.Expiration}, Now = {referenceTimestamp:u}]");

                                LogOutgoing(foundResponse.Value);

                                await foundResponse.Value.WriteToAsync(stream).ConfigureAwait(false);
                                await stream.FlushAsync().ConfigureAwait(false);
                            }
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
            var result = await reader.ReadAsync().ConfigureAwait(false);
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
                var simpleStringValue = Encoding.ASCII.GetString(simpleStringBuffer.AsSpan(1, simpleStringBuffer.Length - 2));

                return new RespSimpleString { Value = simpleStringValue };
            }
            else if (token == ':')
            {
                LogIncoming("Next integer");
                reader.AdvanceTo(consumed);

                var integerBuffer = buffer.ToArray();
                var integerText = Encoding.ASCII.GetString(integerBuffer.AsSpan(1, integerBuffer.Length - 2));
                var integer = long.Parse(integerText);

                return new RespInteger { Value = integer };
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
                reader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            var bulkStringSlice = buffer.Slice(0, length);
            var bulkString = new byte[length];
            bulkStringSlice.CopyTo(bulkString);

            reader.AdvanceTo(bulkStringSlice.End);

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
                reader.AdvanceTo(buffer.Start, buffer.End);
                continue;
            }

            reader.AdvanceTo(buffer.GetPosition(1, lineEnd.Value));

            return respBulkString;
        }
    }
}

public abstract class RespObject
{
    public abstract override string ToString();

    public abstract ValueTask WriteToAsync(Stream target);
}

public sealed class RespArray : RespObject
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

public sealed class RespBulkString : RespObject
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

public sealed class RespNullBulkString : RespObject
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

public sealed class RespSimpleString : RespObject
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

public sealed class RespInteger : RespObject
{
    public required long Value { get; init; }

    public override string ToString()
    {
        return Value.ToString();
    }

    public override async ValueTask WriteToAsync(Stream target)
    {
        await target.WriteAsync(Encoding.ASCII.GetBytes($":{Value}\r\n")).ConfigureAwait(false);
    }
}

public sealed class RespNull : RespObject
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