

using RabbitMQ.Client;
using System.Text;


await Generate_message_wothVirtualHost_user_specific();
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
}
async Task Generate_Log__auto_deleted_queue()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);

			QueueDeclareOk queue = await channel.QueueDeclareAsync(
				queue: "autodeleted-queue",
				durable: true,
				exclusive: false,
				autoDelete: true
				);

			string queueName = queue.QueueName;


			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: queueName,
				body: body
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}
async Task Generate_Log__expier_queue()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);

			QueueDeclareOk queue = await channel.QueueDeclareAsync(
				queue: "expired-queue",
				durable: true,
				exclusive: false,
				autoDelete: false,
					   arguments: new Dictionary<string, object>
						{
							{ "x-expires", 10000 }
						 }
				);
			string queueName = queue.QueueName;


			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: queueName,
				body: body
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log__message_ttl_queue()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);

			QueueDeclareOk queue = await channel.QueueDeclareAsync(
				queue: "message-ttl-queue",
				durable: true,
				exclusive: false,
				autoDelete: false,
					   arguments: new Dictionary<string, object>
						{
							{ "x-message-ttl", 10000 }
						}
				);
			string queueName = queue.QueueName;


			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: queueName,
				body: body
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log__message_dead_queue_letter_exchaneg()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		QueueDeclareOk queue = await channel.QueueDeclareAsync(
		queue: "message-dead-queue",
		durable: true,
		exclusive: false,
		autoDelete: false,
			   arguments: new Dictionary<string, object>
				{
							{ "x-dead-letter-exchange", "routed-exchange" }
				}
		);
		string queueName = queue.QueueName;
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);




			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: queueName,
				body: body
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log__message_dead_letter_routing_key()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		QueueDeclareOk queue = await channel.QueueDeclareAsync(
		queue: "message--qudeadeue",
		durable: true,
		exclusive: false,
		autoDelete: false,
			   arguments: new Dictionary<string, object>
				{
							{ "x-dead-letter-exchange", "routed-exchange-direct" },
				}
		);

		string queueName = queue.QueueName;
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);




			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: queueName,
				body: body
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log__message_mandatory_false()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };
	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync())
	{
		QueueDeclareOk queue = await channel.QueueDeclareAsync(
		queue: "message-dead-queue",
		durable: true,
		exclusive: false,
		autoDelete: false,
			   arguments: new Dictionary<string, object>
				{
							{ "x-dead-letter-exchange", "routed-exchange-direct" },
				}
		);
		channel.BasicReturnAsync += async (sender, ea) =>
		{
			var message = Encoding.UTF8.GetString(ea.Body.ToArray());

			Console.WriteLine("Returned message:");
			Console.WriteLine(message);
		};
		string queueName = queue.QueueName;
		for (int i = 0; i < 10; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);




			await channel.BasicPublishAsync(
				exchange: "",
				routingKey: "fsafsafsf",
				body: body,
				mandatory: true
				);


		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log_message_priorities_consumer()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };

	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
	{
		var queue = await channel.QueueDeclareAsync(
			queue: "q1",
			durable: true,
			exclusive: false,
			autoDelete: false
		);
		await channel.ExchangeDeclareAsync(
					exchange: "exch1",
					type: "direct",
					durable: true,
					autoDelete: false
				);

		await channel.QueueBindAsync(
			queue: queue.QueueName,
			exchange: "exch1",
			routingKey: "key1"
			);

		channel.BasicReturnAsync += async (sender, ea) =>
		{
			var message = Encoding.UTF8.GetString(ea.Body.ToArray());

			Console.WriteLine("Returned message:");
			Console.WriteLine(message);
		};

		channel.BasicAcksAsync += async (sender, ea) =>
		{
			Console.WriteLine("ack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		channel.BasicNacksAsync += async (sender, ea) =>
		{
			Console.WriteLine("Nack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		string queueName = queue.QueueName;
		for (int i = 0; i < 12; i++)
		{

			Thread.Sleep(1000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);


			await channel.BasicPublishAsync(
				exchange: "exch1",
				routingKey: "key1",
				body: body,
				mandatory: true
				);


		}
		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_Log_message_max_length_queue()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };

	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
	{
		var queue = await channel.QueueDeclareAsync(
			queue: "max-queue",
			durable: true,
			exclusive: false,
			autoDelete: false,
			arguments: new Dictionary<string, object?>()
			{
				{ "x-max-length",3},
				{"x-overflow","reject-publish" }
			}
		);
		await channel.ExchangeDeclareAsync(
					exchange: "max-queue-length-exchange",
					type: "direct",
					durable: true,
					autoDelete: false
				);

		await channel.QueueBindAsync(
			queue: queue.QueueName,
			exchange: "max-queue-length-exchange",
			routingKey: "length"
			);

		channel.BasicReturnAsync += async (sender, ea) =>
		{
			var message = Encoding.UTF8.GetString(ea.Body.ToArray());

			Console.WriteLine("Returned message:");
			Console.WriteLine(message);
		};

		channel.BasicAcksAsync += async (sender, ea) =>
		{
			Console.WriteLine("ack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		channel.BasicNacksAsync += async (sender, ea) =>
		{
			Console.WriteLine("Nack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		string queueName = queue.QueueName;
		for (int i = 0; i < 4; i++)
		{

			Thread.Sleep(2000);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);


			await channel.BasicPublishAsync(
				exchange: "max-queue-length-exchange",
				routingKey: "length",
				body: body,
				mandatory: true
				);


		}
		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_persitence_message()
{
	var factory = new ConnectionFactory() { HostName = "localhost" };

	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
	{
		var queue = await channel.QueueDeclareAsync(
			queue: "q2",
			durable: true,
			exclusive: false,
			autoDelete: false

		);
		await channel.ExchangeDeclareAsync(
					exchange: "exch2",
					type: "direct",
					durable: true,
					autoDelete: false
				);

		await channel.QueueBindAsync(
			queue: queue.QueueName,
			exchange: "exch2",
			routingKey: "key"
			);

		channel.BasicReturnAsync += async (sender, ea) =>
		{
			var message = Encoding.UTF8.GetString(ea.Body.ToArray());

			Console.WriteLine("Returned message:");
			Console.WriteLine(message);
		};

		channel.BasicAcksAsync += async (sender, ea) =>
		{
			Console.WriteLine("ack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		channel.BasicNacksAsync += async (sender, ea) =>
		{
			Console.WriteLine("Nack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		for (int i = 0; i < 4; i++)
		{
			Thread.Sleep(500);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);

			await channel.BasicPublishAsync(
				exchange: "exch2",
				routingKey: "key",
				mandatory: true,
				new BasicProperties()
				{
					Persistent = true // if rabbit mq instance get retart or crash the messsages will  stay exist
									  // if it was fasle the message will get deleted
				},
					body: body
				);
		}

		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}

async Task Generate_message_wothVirtualHost_user_specific()
{
	var factory = new ConnectionFactory() { HostName = "localhost",UserName = "maysara", Password = "M592000ysara#" };

	using var connection = await factory.CreateConnectionAsync();

	using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
	{
		var queue = await channel.QueueDeclareAsync(
			queue: "q2",
			durable: true,
			exclusive: false,
			autoDelete: false

		);
		await channel.ExchangeDeclareAsync(
					exchange: "exch2",
					type: "direct",
					durable: true,
					autoDelete: false
				);

		await channel.QueueBindAsync(
			queue: queue.QueueName,
			exchange: "exch2",
			routingKey: "key"
			);

		channel.BasicReturnAsync += async (sender, ea) =>
		{
			var message = Encoding.UTF8.GetString(ea.Body.ToArray());

			Console.WriteLine("Returned message:");
			Console.WriteLine(message);
		};

		channel.BasicAcksAsync += async (sender, ea) =>
		{
			Console.WriteLine("ack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		channel.BasicNacksAsync += async (sender, ea) =>
		{
			Console.WriteLine("Nack message:");
			Console.WriteLine(ea.DeliveryTag);
		};

		for (int i = 0; i < 4; i++)
		{
			Thread.Sleep(500);
			var message = $"log info {i}";
			var body = Encoding.UTF8.GetBytes(message);

			await channel.BasicPublishAsync(
				exchange: "exch2",
				routingKey: "key",
				mandatory: true,
				new BasicProperties()
				{
					Persistent = true // if rabbit mq instance get retart or crash the messsages will  stay exist
									  // if it was fasle the message will get deleted
				},
					body: body
				);
		}
		
		Console.WriteLine("press enter to exist");
		Console.ReadLine();
	}
}



