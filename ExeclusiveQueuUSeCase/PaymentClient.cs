using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

internal static class PaymentClient
{
	public static async Task RunAsync(string instanceId)
	{
		var factory = new ConnectionFactory { HostName = FraudCheckRpc.HostName };
		await using var connection = await factory.CreateConnectionAsync();
		await using var channel = await connection.CreateChannelAsync();

		var replyQueue = await channel.QueueDeclareAsync(
			queue: string.Empty,
			durable: false,
			exclusive: true,
			autoDelete: true);

		var pendingResponses = new ConcurrentDictionary<string, TaskCompletionSource<string>>();

		var consumer = new AsyncEventingBasicConsumer(channel);
		consumer.ReceivedAsync += async (_, ea) =>
		{
			var correlationId = ea.BasicProperties.CorrelationId;
			var response = Encoding.UTF8.GetString(ea.Body.Span);

			if (!string.IsNullOrEmpty(correlationId) &&
			    pendingResponses.TryRemove(correlationId, out var completion))
				completion.TrySetResult(response);

			await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
		};

		await channel.BasicConsumeAsync(
			queue: replyQueue.QueueName,
			autoAck: false,
			consumer: consumer);

		Console.WriteLine($"Payment API instance [{instanceId}] started.");
		Console.WriteLine($"Exclusive reply queue: {replyQueue.QueueName}");
		Console.WriteLine("Only this connection can read replies from this queue.");
		Console.WriteLine();

		var cards = new[]
		{
			"4111-1111-1111-4242",
			"5500-0000-0000-stolen",
			"3782-8224-6310-005",
			"4000-0000-0000-0002"
		};

		foreach (var card in cards)
		{
			var correlationId = $"{instanceId}-{Guid.NewGuid():N}";
			var completion = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
			pendingResponses[correlationId] = completion;

			var requestProps = new BasicProperties
			{
				CorrelationId = correlationId,
				ReplyTo = replyQueue.QueueName
			};

			Console.WriteLine($"[{instanceId}] Sending fraud check for card ...{card[^4..]}");

			await channel.BasicPublishAsync(
				exchange: string.Empty,
				routingKey: FraudCheckRpc.WorkQueue,
				mandatory: false,
				basicProperties: requestProps,
				body: Encoding.UTF8.GetBytes(card));

			using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
			var response = await completion.Task.WaitAsync(timeout.Token);

			Console.WriteLine($"[{instanceId}] Received reply: {response}");
			Console.WriteLine();
		}

		Console.WriteLine($"[{instanceId}] All payments processed. Press Enter to close connection.");
		Console.WriteLine("The exclusive reply queue will be deleted when this connection closes.");
		Console.ReadLine();
	}
}
