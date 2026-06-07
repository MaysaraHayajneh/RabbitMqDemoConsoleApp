using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services
{
	internal static class MessageTracingUseCaseService
	{
		public async static Task WriteMessage()
		{
			{
				var factory = new ConnectionFactory() { HostName = "localhost" };
				using var connection = await factory.CreateConnectionAsync();

				using (var channel = await connection.CreateChannelAsync())
				{
					for (int i = 0; i < 10; i++)
					{

						Thread.Sleep(300);
						var message = $"log info {i}";
						var body = Encoding.UTF8.GetBytes(message);



						await channel.BasicPublishAsync(
							exchange: "amq.direct",
							routingKey: "q1",
							body: body
							);


					}

					Console.WriteLine("press enter to exist");
					Console.ReadLine();
				}
			}
		}
	}
}
