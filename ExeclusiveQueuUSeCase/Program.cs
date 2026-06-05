if (args.Length == 0)
{
	PrintUsage();
	return;
}

switch (args[0].ToLowerInvariant())
{
	case "fraud":
		await FraudDetectionService.RunAsync();
		break;
	case "payment":
		var instanceId = args.Length > 1 ? args[1] : "A";
		await PaymentClient.RunAsync(instanceId);
		break;
	default:
		PrintUsage();
		break;
}

static void PrintUsage()
{
	Console.WriteLine("Exclusive Queue RPC Demo - Payment / Fraud Check");
	Console.WriteLine();
	Console.WriteLine("Why exclusive queues?");
	Console.WriteLine("  Each payment instance gets a private, temporary reply inbox.");
	Console.WriteLine("  Only that connection can consume replies. The queue auto-deletes on disconnect.");
	Console.WriteLine();
	Console.WriteLine("How to run (requires RabbitMQ on localhost):");
	Console.WriteLine("  Terminal 1: dotnet run -- fraud");
	Console.WriteLine("  Terminal 2: dotnet run -- payment A");
	Console.WriteLine("  Terminal 3: dotnet run -- payment B   (optional - second instance, own reply queue)");
}
