using System.Collections.Immutable;
using System.Text;

namespace codecrafters_redis.src
{
    public abstract class RespObject
    {
        public abstract override string ToString();

        public abstract ValueTask WriteToAsync(Stream target);
    }

    public sealed class RespArrayObject : RespObject
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

    public sealed class RespNullArrayObject : RespObject
    {
        private static readonly byte[] Payload = Encoding.ASCII.GetBytes("*-1\r\n");

        private RespNullArrayObject()
        {
        }

        public static RespNullArrayObject Instance { get; } = new();

        public override string ToString()
        {
            return "<null-array>";
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            await target.WriteAsync(Payload).ConfigureAwait(false);
        }
    }

    public sealed class RespBulkStringObject : RespObject
    {
        private static readonly byte[] EndOfPart = Encoding.ASCII.GetBytes("\r\n");

        public required ReadOnlyMemory<byte> Value { get; init; }

        public string AsText()
        {
            return Encoding.ASCII.GetString(Value.Span);
        }

        public override string ToString()
        {
            return Encoding.ASCII.GetString(Value.Span);
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            var preamble = Encoding.ASCII.GetBytes($"${Value.Length}\r\n");

            await target.WriteAsync(preamble).ConfigureAwait(false);
            await target.WriteAsync(Value).ConfigureAwait(false);
            await target.WriteAsync(EndOfPart).ConfigureAwait(false);
        }
    }

    public sealed class RespNullBulkStringObject : RespObject
    {
        private static readonly byte[] Payload = Encoding.ASCII.GetBytes("$-1\r\n");

        private RespNullBulkStringObject()
        {
        }

        public static RespNullBulkStringObject Instance { get; } = new();

        public override string ToString()
        {
            return "<null-blk>";
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            await target.WriteAsync(Payload).ConfigureAwait(false);
        }
    }

    public sealed class RespSimpleStringObject : RespObject
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

    public sealed class RespIntegerObject : RespObject
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

    public sealed class RespNullObject : RespObject
    {
        private static readonly byte[] Payload = Encoding.ASCII.GetBytes("_\r\n");

        private RespNullObject()
        {
        }

        public static RespNullObject Instance { get; } = new();

        public override string ToString()
        {
            return "<null>";
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            await target.WriteAsync(Payload).ConfigureAwait(false);
        }
    }

    public sealed class RespSimpleError : RespObject
    {
        public required string Value { get; init; }

        public override string ToString()
        {
            return Value;
        }

        public override async ValueTask WriteToAsync(Stream target)
        {
            await target.WriteAsync(Encoding.ASCII.GetBytes($"-{Value}\r\n"));
        }
    }
}
