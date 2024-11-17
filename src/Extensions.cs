using System.Buffers;
using System.Text;

namespace codecrafters_redis.src
{
    internal static class Extensions
    {
        public static string ToDebugString(this ReadOnlySequence<byte> sequence)
        {
            var debugText = Encoding.ASCII
                .GetString(sequence.ToArray())
                .Replace("\r", "\\r")
                .Replace("\n", "\\n");

            return debugText;
        }
    }
}
