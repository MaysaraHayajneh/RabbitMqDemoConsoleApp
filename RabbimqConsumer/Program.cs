using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;
await ConumeMessages();
async Task ConumeMessages()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using (var connection = await factory.CreateConnectionAsync())
	{
		using (var channel = await connection.CreateChannelAsync())
		{
			var consumer = new AsyncEventingBasicConsumer(channel);
			consumer.ReceivedAsync += async (model, ea) =>
			{
				var body = ea.Body.ToArray();
				var message = Encoding.UTF8.GetString(body);
				Console.WriteLine($"{message} has been cosumed by {ea.ConsumerTag}");
				Console.WriteLine($"===================================================");
				await channel.BasicAckAsync(
					deliveryTag: ea.DeliveryTag,
					multiple: false
					);
			};

			await channel.BasicConsumeAsync(
				queue: "loginfo",
				autoAck: false,
				consumer: consumer,
				consumerTag: "info"
				);
			await channel.BasicConsumeAsync(
				queue: "logerror",
				autoAck: false,
				consumer: consumer,
				consumerTag: "error"
				);
			await channel.BasicConsumeAsync(
				queue: "logall",
				autoAck: false,
				consumer: consumer,
				consumerTag: "all"
				);


			Console.WriteLine("press enter to exit");
			Console.ReadKey();
		}
	}
}