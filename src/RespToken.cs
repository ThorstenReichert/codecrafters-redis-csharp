namespace codecrafters_redis.src
{
    public abstract class RespToken
    {
        public abstract override string ToString();
    }

    public sealed class RespIntegerToken : RespToken
    {
        public required long Value { get; init; }

        public override string ToString() => $"Token/Integer [Value = {Value}]";
    }

    public sealed class RespSimpleStringToken : RespToken
    {
        public required string Value { get; init; }

        public override string ToString() => $"Token/SimpleString [ Value = {Value}]";
    }

    public sealed class RespBulkStringToken : RespToken
    {
        public required ReadOnlyMemory<byte> Value { get; init; }

        public override string ToString() => $"Token/BulkString [Length = {Value.Length}]";
    }

    public sealed class RespNullBulkStringToken : RespToken
    {
        private RespNullBulkStringToken()
        {

        }

        public static RespNullBulkStringToken Instance { get; } = new();

        public override string ToString() => "Token/NullBulkString";
    }

    public sealed class RespArrayStartToken : RespToken
    {
        public required int Length { get; init; }

        public override string ToString() => $"Token/ArrayStart [Length = {Length}]";
    }

    public sealed class RespNullArrayToken : RespToken
    {
        private RespNullArrayToken()
        {
        }

        public static RespNullArrayToken Instance { get; } = new();

        public override string ToString() => "Token/NullArray";
    }
}
