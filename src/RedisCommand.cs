using System.Threading.Channels;

namespace codecrafters_redis.src
{
    public sealed record class RedisCommand(int ClientId, RespObject CommandObject, ChannelWriter<RespObject> ReplyTo);
}
