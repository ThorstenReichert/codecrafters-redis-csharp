using System.Net;
using System.Net.Sockets;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
using TcpListener server = new(IPAddress.Any, 6379);
server.Start();

var clientCounter = 0;

while (true)
{
    var handler = await server.AcceptTcpClientAsync().ConfigureAwait(false);

    _ = ServeTcpClientAsync(handler, clientCounter++);
}

static async Task ServeTcpClientAsync(TcpClient handler, int id)
{
    void LogIncoming(string message) => Console.WriteLine($"{id,-3} >> {message}");
    void LogOutgoing(string message) => Console.WriteLine($"{id,-3} << {message}");
    void LogError(Exception error) => Console.WriteLine($"{id} !! {error}");

    try
    {
        using var stream = handler.GetStream();
        using var reader = new StreamReader(stream);
        using var writer = new StreamWriter(stream) { NewLine = "\r\n" };

        while (await reader.ReadLineAsync().ConfigureAwait(false) is string command)
        {
            LogIncoming(command);
            if (command.Equals("PING", StringComparison.OrdinalIgnoreCase))
            {
                var response = "+PONG";

                LogOutgoing(response);

                await writer.WriteLineAsync(response);
                await writer.FlushAsync().ConfigureAwait(false);
            }
        }
    }
    catch (Exception e) when (e is not OperationCanceledException)
    {
        LogError(e);
    }
}
