using codecrafters_redis.src;
using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Channels;

internal class Program
{
    private static async Task Main()
    {
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

            _ = new RedisClientHolder(handler, keyValueStore, clientCounter++).RunAsync();
        }
    }
}

/// <remarks>
///     Allows to separate tcp client from actual implementation (which accepts the underlying stream for testing)
///     while still cleaning up connections on failures.
/// </remarks>
internal sealed class RedisClientHolder(TcpClient redisClient, ConcurrentDictionary<string, (RespObject Value, DateTime Expiration)> keyValueStore, int id)
{
    public async Task RunAsync()
    {
        using var redisClientStream = redisClient.GetStream();

        await new RedisClientHandler(redisClientStream, keyValueStore, id).RunAsync().ConfigureAwait(false);
    }
}

public sealed class RedisClientHandler(Stream redisClientStream, ConcurrentDictionary<string, (RespObject Value, DateTime Expiration)> keyValueStore, int id)
{
    private static readonly ReadOnlyMemory<byte> CmdPing = Encoding.ASCII.GetBytes("PING");
    private static readonly ReadOnlyMemory<byte> CmdEcho = Encoding.ASCII.GetBytes("ECHO");
    private static readonly ReadOnlyMemory<byte> CmdSet = Encoding.ASCII.GetBytes("SET");
    private static readonly ReadOnlyMemory<byte> CmdGet = Encoding.ASCII.GetBytes("GET");
    private static readonly ReadOnlyMemory<byte> OptPx = Encoding.ASCII.GetBytes("px");
    private static readonly ReadOnlyMemory<byte> RespPartEnd = Encoding.ASCII.GetBytes("\r\n");

    private void LogIncoming(RespToken? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogIncoming(RespObject? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogIncoming(string? message) => Console.WriteLine($"{id,3} >> {message}");
    private void LogOutgoing(RespObject? message) => Console.WriteLine($"{id,3} << {message}");
    private void LogOutgoing(string? message) => Console.WriteLine($"{id,3} << {message}");
    private void LogError(Exception? error) => Console.WriteLine($"{id,3} !! {error}");
    private void LogError(string? error) => Console.WriteLine($"{id,3} !! {error}");

    public async Task RunAsync()
    {
        var tokenChannel = Channel.CreateBounded<RespToken>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        var objectChannel = Channel.CreateBounded<RespObject>(new BoundedChannelOptions(1024)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = true,
        });

        try
        {
            await using var stream = redisClientStream;
            var reader = PipeReader.Create(stream);

            _ = Task.Run(() => ReadRespTokensAsync(reader, tokenChannel.Writer));
            _ = Task.Run(() => ReadRespObjectsAsync(tokenChannel.Reader, objectChannel.Writer));

            while (await objectChannel.Reader.WaitToReadAsync().ConfigureAwait(false))
            {
                while (objectChannel.Reader.TryRead(out var command))
                {
                    await HandleRedisCommandAsync(stream, command).ConfigureAwait(false);
                }
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            LogError(e);
        }
        finally
        {
            tokenChannel.Writer.TryComplete();
            objectChannel.Writer.TryComplete();
        }
    }

    /// <summary>
    ///     Read bytes from <paramref name="reader"/>, parse RESP-tokens from it, and push the tokens into <paramref name="writer"/>.
    /// </summary>
    private async Task ReadRespTokensAsync(PipeReader reader, ChannelWriter<RespToken> writer)
    {
        try
        {
            while (true)
            {
                var result = await reader.ReadAsync().ConfigureAwait(false);
                var buffer = result.Buffer;

                try
                {

                    while (TryReadRespToken(ref buffer, out var token))
                    {
                        LogIncoming(token);

                        while (await writer.WaitToWriteAsync().ConfigureAwait(false))
                        {
                            if (writer.TryWrite(token))
                            {
                                break;
                            }
                        }
                    }

                    if (result.IsCompleted)
                    {
                        if (buffer.Length > 0)
                        {
                            LogError($"Reader completed with pending bytes [Count = {buffer.Length}]");
                        }

                        break;
                    }
                }
                finally
                {
                    reader.AdvanceTo(buffer.Start, buffer.End);
                }
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            writer.TryComplete(e);
        }
        finally
        {
            writer.TryComplete();
        }
    }

    /// <summary>
    ///     Read a single RESP-token from <paramref name="buffer"/> and trim the corresponding RESP parts from <paramref name="buffer"/>.
    /// </summary>
    private bool TryReadRespToken(ref ReadOnlySequence<byte> buffer, [NotNullWhen(true)] out RespToken? respToken)
    {
        var lineEnd = buffer.PositionOf((byte) '\n');
        if (!lineEnd.HasValue)
        {
            respToken = default;
            return false;
        }

        // Entire line including \r\n terminator.
        var line = buffer.Slice(0, buffer.GetPosition(1, lineEnd.Value));
        var reader = new SequenceReader<byte>(line);

        if (!reader.TryRead(out byte typeToken))
        {
            respToken = default;
            return false;
        }

        if (typeToken == (byte) '+')
        {

            if (!reader.TryReadTo(out ReadOnlySequence<byte> simpleStringBuffer, RespPartEnd.Span))
            {
                throw new InvalidFormatException("RESP part was not terminated with '\\r\\n'");
            }

            respToken = new RespSimpleStringToken { Value = Encoding.ASCII.GetString(simpleStringBuffer) };
            buffer = buffer.Slice(line.End, buffer.End);
            return true;
        }
        else if (typeToken == (byte) ':')
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> integerBuffer, RespPartEnd.Span))
            {
                throw new InvalidFormatException("RESP part was not terminated with '\\r\\n'");
            }

            // Maximum number of digits of a 64bit integer string in base 10 (including sign).
            const int MaxLength = 20;

            if (integerBuffer.Length > MaxLength)
            {
                throw new InvalidFormatException($"RESP integer has more digits than allowed [Count = {integerBuffer.Length}, MaxCount = {MaxLength}]");
            }

            Span<byte> singleSliceIntegerBuffer = stackalloc byte[MaxLength];
            integerBuffer.CopyTo(singleSliceIntegerBuffer);

            respToken = new RespIntegerToken { Value = long.Parse(singleSliceIntegerBuffer[..(int) integerBuffer.Length]) };
            buffer = buffer.Slice(line.End, buffer.End);
            return true;
        }
        else if (typeToken == (byte) '*')
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> arrayBuffer, RespPartEnd.Span))
            {
                throw new InvalidFormatException("RESP part was not terminated with '\\r\\n'");
            }

            // Maximum number of digits of an array-length specifier.
            const int MaxLength = 20;

            if (arrayBuffer.Length > MaxLength)
            {
                throw new InvalidFormatException($"RESP array length has more digits than allowed [Count = {arrayBuffer.Length}, MaxCount = {MaxLength}]");
            }

            Span<byte> singleSliceArrayBuffer = stackalloc byte[MaxLength];
            arrayBuffer.CopyTo(singleSliceArrayBuffer);

            var arrayLength = int.Parse(singleSliceArrayBuffer[..(int) arrayBuffer.Length]);
            respToken = arrayLength < 0
                ? RespNullArrayToken.Instance
                : new RespArrayStartToken { Length = arrayLength };
            buffer = buffer.Slice(line.End, buffer.End);
            return true;
        }
        else if (typeToken == (byte) '$')
        {
            if (!reader.TryReadTo(out ReadOnlySequence<byte> stringLengthBuffer, RespPartEnd.Span))
            {
                throw new InvalidFormatException("RESP part was not terminated with '\\r\\n'");
            }

            // Maximum number of digits of an bulk-string-length specifier.
            const int MaxLength = 20;

            if (stringLengthBuffer.Length > MaxLength)
            {
                throw new InvalidFormatException($"RESP array length has more digits than allowed [Count = {stringLengthBuffer.Length}, MaxCount = {MaxLength}]");
            }

            Span<byte> singleSliceStringLengthBuffer = stackalloc byte[MaxLength];
            stringLengthBuffer.CopyTo(singleSliceStringLengthBuffer);

            var stringLength = int.Parse(singleSliceStringLengthBuffer[..(int) stringLengthBuffer.Length]);
            if (stringLength < 0)
            {
                respToken = RespNullBulkStringToken.Instance;
                buffer = buffer.Slice(line.End, buffer.End);
                return true;
            }
            else
            {
                var requiredBulkStringBufferLength = line.Length + stringLength + RespPartEnd.Length;

                if (buffer.Length < requiredBulkStringBufferLength)
                {
                    // The buffer does not contain enough information to extract the string.
                    // We need at least the length-specification part, the therein specified number of bytes, and the part terminator.
                    // Do not advance the buffer since we will need the length part when trying again next time.

                    respToken = default;
                    return false;
                }
                var stringReader = new SequenceReader<byte>(buffer);

                // Advance past the end of the string length part.
                stringReader.Advance(line.Length);

                // Extract number of bytes as specified in the string length part;
                var singleSliceStringBuffer = new byte[stringLength];
                var stringReadSuccess = stringReader.TryReadExact(stringLength, out ReadOnlySequence<byte> stringBuffer);
                Debug.Assert(stringReadSuccess);

                stringBuffer.CopyTo(singleSliceStringBuffer);

                // Check part terminator for consistency.
                Span<byte> singleSlicePartEndBuffer = stackalloc byte[RespPartEnd.Length];
                var partEndReadSuccess = stringReader.TryReadExact(RespPartEnd.Length, out ReadOnlySequence<byte> partEndBuffer);
                Debug.Assert(partEndReadSuccess);

                partEndBuffer.CopyTo(singleSlicePartEndBuffer);

                if (!singleSlicePartEndBuffer.SequenceEqual(RespPartEnd.Span))
                {
                    buffer = buffer.Slice(requiredBulkStringBufferLength);
                    throw new InvalidFormatException("RESP bulk string was malformed (terminator mismatch). Buffer has been forwarded to skip the current object.");
                }

                respToken = new RespBulkStringToken { Value = singleSliceStringBuffer };
                buffer = buffer.Slice(requiredBulkStringBufferLength);
                return true;
            }
        }
        else
        {
            buffer = buffer.Slice(line.End, buffer.End);
            throw new NotImplementedException($"No implementation for parsing types with type token '{typeToken}'. Buffer has been forwarded to skip the current object.");
        }
    }

    /// <summary>
    ///     Read tokens from <paramref name="reader"/>, materialize RESP-objects from them, and push the objects into <paramref name="writer"/>.
    /// </summary>
    private async Task ReadRespObjectsAsync(ChannelReader<RespToken> reader, ChannelWriter<RespObject> writer)
    {
        async Task WriteRespObjectAsync(ChannelWriter<RespObject> writer, RespObject respObject)
        {
            while (await writer.WaitToWriteAsync().ConfigureAwait(false))
            {
                if (writer.TryWrite(respObject))
                {
                    return;
                }
            }

            LogError($"Unconsumed RESP object of type {respObject.GetType().Name}");
        }

        try
        {
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                if (await TryReadRespObjectAsync(reader) is RespObject respObject)
                {
                    await WriteRespObjectAsync(writer, respObject).ConfigureAwait(false);
                }
                else
                {
                    // null indicates end-of-stream.
                    return;
                }
            }
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            writer.TryComplete(e);
        }
        finally
        {
            writer.TryComplete();
        }
    }

    /// <summary>
    ///     Read a single RESP-object from <paramref name="reader"/>. Returns <c>null</c> when <paramref name="reader"/> is completed.
    /// </summary>
    private async Task<RespObject?> TryReadRespObjectAsync(ChannelReader<RespToken> reader)
    {
        static async Task<RespToken?> NextTokenAsync(ChannelReader<RespToken> reader)
        {
            while (await reader.WaitToReadAsync().ConfigureAwait(false))
            {
                if (reader.TryRead(out var token))
                {
                    return token;
                }
            }

            return null;
        }

        var token = await NextTokenAsync(reader).ConfigureAwait(false);

        if (token is null)
        {
            return null;
        }
        else if (token is RespNullArrayToken)
        {
            return RespNullArrayObject.Instance;
        }
        else if (token is RespNullBulkStringToken)
        {
            return RespNullBulkStringObject.Instance;
        }
        else if (token is RespSimpleStringToken simpleStringToken)
        {
            return new RespSimpleStringObject { Value = simpleStringToken.Value };
        }
        else if (token is RespIntegerToken integerToken)
        {
            return new RespIntegerObject { Value = integerToken.Value };
        }
        else if (token is RespBulkStringToken bulkStringToken)
        {
            return new RespBulkStringObject { Value = bulkStringToken.Value };
        }
        else if (token is RespArrayStartToken arrayStartToken)
        {
            var array = new RespObject[arrayStartToken.Length];
            for (var i = 0; i < array.Length; i++)
            {
                array[i] = await TryReadRespObjectAsync(reader)
                    ?? throw new InvalidFormatException("RESP token stream ended before RESP array was complete");
            }

            return new RespArrayObject { Items = [.. array] };
        }
        else
        {
            LogError($"Unknown RESP token of type {token.GetType().Name}");
            return null;
        }
    }

    /// <summary>
    ///     Handles any command specified by <paramref name="command"/> and writes the response into <paramref name="stream"/>.
    /// </summary>
    private async Task HandleRedisCommandAsync(Stream stream, RespObject command)
    {
        LogIncoming(command);

        if (command is RespArrayObject respArray)
        {
            if (respArray.Items is [RespBulkStringObject pingCmd]
                && pingCmd.Value.Span.SequenceEqual(CmdPing.Span))
            {
                var response = new RespSimpleStringObject { Value = "PONG" };

                LogOutgoing(response);

                await response.WriteToAsync(stream).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
            }

            else if (respArray.Items is [RespBulkStringObject echoCmd, RespBulkStringObject echoArg]
                && echoCmd.Value.Span.SequenceEqual(CmdEcho.Span))
            {
                var response = echoArg;

                LogOutgoing(response);

                await response.WriteToAsync(stream).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
            }

            else if (respArray.Items is [RespBulkStringObject setCmd, RespBulkStringObject setName, RespObject setValue, .. var setOptional]
                && setCmd.Value.Span.SequenceEqual(CmdSet.Span))
            {
                if (setOptional is [RespBulkStringObject pxOption, RespBulkStringObject pxTimeout]
                    && pxOption.Value.Span.SequenceEqual(OptPx.Span))
                {
                    var expirationDuration = int.Parse(pxTimeout.Value.Span);
                    var expiration = DateTime.UtcNow + TimeSpan.FromMilliseconds(expirationDuration);

                    LogOutgoing($"Expires [UTC = {expiration:u}]");

                    keyValueStore[setName.AsText()] = (setValue, expiration);
                }
                else
                {
                    keyValueStore[setName.AsText()] = (setValue, DateTime.MaxValue);
                }

                var response = new RespSimpleStringObject { Value = "OK" };

                LogOutgoing(response);

                await response.WriteToAsync(stream).ConfigureAwait(false);
                await stream.FlushAsync().ConfigureAwait(false);
            }

            else if (respArray.Items is [RespBulkStringObject getCmd, RespBulkStringObject getName]
                && getCmd.Value.Span.SequenceEqual(CmdGet.Span))
            {
                if (keyValueStore.TryGetValue(getName.AsText(), out var foundResponse))
                {
                    var referenceTimestamp = DateTime.UtcNow;
                    if (foundResponse.Expiration < referenceTimestamp)
                    {
                        LogOutgoing($"Expired [UTC = {foundResponse.Expiration:u}, Now = {referenceTimestamp:u}]");

                        var expiredResponse = RespNullBulkStringObject.Instance;

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
                    var notFoundResponse = RespNullBulkStringObject.Instance;

                    LogOutgoing(notFoundResponse);

                    await notFoundResponse.WriteToAsync(stream).ConfigureAwait(false);
                    await stream.FlushAsync().ConfigureAwait(false);
                }
            }
        }
    }
}