using System.Net;
using System.Net.Sockets;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
using TcpListener server = new(IPAddress.Any, 6379);
server.Start();
var handler = await server.AcceptTcpClientAsync().ConfigureAwait(false); // wait for client

using var stream = handler.GetStream();
using var reader = new StreamReader(stream);
using var writer = new StreamWriter(stream) { NewLine = "\r\n" };

while (await reader.ReadLineAsync().ConfigureAwait(false) is string command)
{
    Console.WriteLine($"Rcv: {command}");
    await writer.WriteLineAsync("+PONG");
}