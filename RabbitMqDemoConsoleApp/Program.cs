

using RabbitMQ.Client;
using System.Text;


await Generate_Log_TopicExchange();
async Task Generate_Log_TopicExchange()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		for (int i = 0; i < 1000; i++)
		{
			if (i % 2 == 0)
			{
				Thread.Sleep(2000);
				var message = $"log info {i}";
				var body = Encoding.UTF8.GetBytes(message);

				await channel.BasicPublishAsync(
					exchange: "amq.topic",
					routingKey: "log.info",
					body: body
					);
			}
			else
			{
				Thread.Sleep(2000);
				var message = $"log info {i}";
				var body = Encoding.UTF8.GetBytes(message);

				await channel.BasicPublishAsync(
					exchange: "amq.topic",
					routingKey: "log.error",
					body: body
					);
			}

		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
	;
}