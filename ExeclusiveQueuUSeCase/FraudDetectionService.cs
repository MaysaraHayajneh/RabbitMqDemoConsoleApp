using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

internal static class FraudDetectionService
{
	public static async Task RunAsync()
	{
		var factory = new ConnectionFactory { HostName = FraudCheckRpc.HostName };
		await using var connection = await factory.CreateConnectionAsync();
		await using var channel = await connection.CreateChannelAsync();

		await channel.QueueDeclareAsync(
			queue: FraudCheckRpc.WorkQueue,
			durable: true,
			exclusive: false,
			autoDelete: false);

		await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

		Console.WriteLine("Fraud Detection Service is running.");
		Console.WriteLine($"Listening on queue: {FraudCheckRpc.WorkQueue}");
		Console.WriteLine("Press Ctrl+C to stop.");
		Console.WriteLine();

		var consumer = new AsyncEventingBasicConsumer(channel);
		consumer.ReceivedAsync += async (_, ea) =>
		{
			var cardNumber = Encoding.UTF8.GetString(ea.Body.Span);
			var replyTo = ea.BasicProperties.ReplyTo;
			var correlationId = ea.BasicProperties.CorrelationId;

			if (string.IsNullOrEmpty(replyTo) || string.IsNullOrEmpty(correlationId))
			{
				Console.WriteLine("[Fraud] Skipping message: missing ReplyTo or CorrelationId.");
				await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
				return;
			}

			Console.WriteLine($"[Fraud] Checking card ending ...{cardNumber[^4..]} (correlation: {correlationId})");

			await Task.Delay(500);

			var approved = !cardNumber.Contains("stolen", StringComparison.OrdinalIgnoreCase);
			var response = approved
				? $"APPROVED - card ...{cardNumber[^4..]} passed fraud checks"
				: $"DECLINED - card ...{cardNumber[^4..]} flagged as stolen";

			var replyProps = new BasicProperties { CorrelationId = correlationId };

			await channel.BasicPublishAsync(
				exchange: string.Empty,
				routingKey: replyTo,
				mandatory: false,
				basicProperties: replyProps,
				body: Encoding.UTF8.GetBytes(response));

			Console.WriteLine($"[Fraud] Sent {response.Split('-')[0].Trim()} to exclusive reply queue: {replyTo}");
			Console.WriteLine();

			await channel.BasicAckAsync(ea.DeliveryTag, multiple: false);
		};

		await channel.BasicConsumeAsync(
			queue: FraudCheckRpc.WorkQueue,
			autoAck: false,
			consumer: consumer);

		using var stopSignal = new CancellationTokenSource();
		Console.CancelKeyPress += (_, e) =>
		{
			e.Cancel = true;
			stopSignal.Cancel();
		};

		try
		{
			await Task.Delay(Timeout.Infinite, stopSignal.Token);
		}
		catch (OperationCanceledException)
		{
		}
	}
}
