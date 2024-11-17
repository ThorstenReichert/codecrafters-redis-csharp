using System.Buffers;
using System.Collections.Immutable;
using System.Diagnostics;
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

while (true)
{
    var handler = await server.AcceptTcpClientAsync().ConfigureAwait(false);

    _ = new RedisClientHandler(handler, clientCounter++).RunAsync();
}

internal sealed class RedisClientHandler(TcpClient tcpClient, int id)
{
    private static readonly byte[] CmdPing = Encoding.ASCII.GetBytes("PING");
    private static readonly byte[] CmdEcho = Encoding.ASCII.GetBytes("ECHO");

    private void LogIncoming(RespObject? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogIncoming(string? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogOutgoing(RespObject? message) => Console.WriteLine($"{id,3} << {message}");
    private void LogError(Exception? error) => Console.WriteLine($"{id,3} !! {error}");
    private void LogError(string? error) => Console.WriteLine($"{id,3} !! {error}");

    public async Task RunAsync()
    {
        try
        {
            using var stream = tcpClient.GetStream();

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

            var lineEnd = buffer.PositionOf((byte) '\n');
            if (!lineEnd.HasValue)
            {
                continue;
            }

            var token = ParseTypeToken(ref buffer);

            if (token == '+')
            {
                LogIncoming("Next simple string");
                reader.AdvanceTo(lineEnd.Value);

                var simpleStringBuffer = buffer.ToArray();
                var simpleStringValue = Encoding.UTF8.GetString(simpleStringBuffer.AsSpan(1, simpleStringBuffer.Length - 2));

                return new RespSimpleString { Value = simpleStringValue };
            }
            else if (token == '$')
            {
                LogIncoming("Next bulk string");

                var length = ParseBulkStringLength(ref buffer);
                reader.AdvanceTo(lineEnd.Value);

                return await NextRespBulkStringAsync(reader, length);
            }
            else if (token == '*')
            {
                LogIncoming("Next array");

                var length = ParseArrayLength(buffer);
                var builder = ImmutableArray.CreateBuilder<RespObject>(length);

                reader.AdvanceTo(lineEnd.Value);

                for (var i = 0; i < length; i++)
                {
                    builder[i] = await NextRespObjectAsync(reader);
                }

                return new RespArray { Items = builder.ToImmutable() };
            }
            else
            {
                reader.AdvanceTo(lineEnd.Value);

                var objectText = Encoding.ASCII.GetString(buffer.ToArray()).Replace("\r\n", "\\r\\n");
                throw new NotImplementedException($"Unrecognized object [Bytes = {objectText}]");
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

    static int ParseBulkStringLength(ref readonly ReadOnlySequence<byte> buffer)
    {
        Debug.Assert(buffer.Length > 0, "BulkString length part must not be empty");
        Debug.Assert(buffer.FirstSpan[0] == '$', "BulkString length part must start with '$'");

        var reader = new SequenceReader<byte>(buffer);
        reader.Advance(1);

        if (!reader.TryReadTo(out ReadOnlySequence<byte> payload, (byte) '\r', false))
        {
            throw new InvalidOperationException("BulkString preamble is missing line-end");
        }

        return ParseInteger(payload);
    }

    static int ParseArrayLength(ReadOnlySequence<byte> buffer)
    {
        Debug.Assert(buffer.Length > 0, "BulkString length part must not be empty");
        Debug.Assert(buffer.FirstSpan[0] == '*', "BulkString length part must start with '*'");

        var reader = new SequenceReader<byte>(buffer);
        reader.Advance(1);

        if (!reader.TryReadTo(out ReadOnlySequence<byte> payload, (byte) '\r', false))
        {
            throw new InvalidOperationException("BulkString preamble is missing line-end");
        }

        return ParseInteger(payload);
    }

    static int ParseInteger(ReadOnlySequence<byte> buffer)
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
                continue;
            }

            reader.AdvanceTo(lineEnd.Value);

            return respBulkString;
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
        public required byte[] Value { get; init; }

        public override string ToString()
        {
            return Encoding.ASCII.GetString(Value);
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            var payload = Encoding.ASCII.GetBytes($"${Value.Length}\r\n{Value}\r\n");

            await target.WriteAsync(payload).ConfigureAwait(false);
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