using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Net;
using System.Text;

namespace RabbitMqStream.Services
{
	public static class ReadFromStreamByOffsetService
	{
		public static async Task ReadFromStream_By_Offset()
		{

			var config = new StreamSystemConfig()
			{
				Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) }, // rabbit ,q stream port
				UserName = "maysara",
				Password = "maysara"
			};


			var streamSystem = await StreamSystem.Create(config);

			const string streamName = "stream-q";
			const string consumerReference = "DOTNET 10";
			ulong offset = ulong.MinValue; // the offset to start reading from, you can set this to a specific value or use the default behavior
			var consumerConfig = new ConsumerConfig(streamSystem, streamName)
			{
				Reference = consumerReference,

				//OffsetSpec = new OffsetTypeOffset(), if this did not get determinde will read from the next by default

				//OffsetSpec = new OffsetTypeFirst(), // read from the start of the stream

				//OffsetSpec = new OffsetTypeLast(), // read from the last of the stream

				// recive teh message event 

				MessageHandler = async (sourceStream, consumer, ctx, message) =>
				{
					Console.WriteLine($"message coming form {sourceStream} daata :{Encoding.Default.GetString(message.Data.Contents.ToArray())}  consumed {consumer.Info.Identifier}");

					// after procsessiong store teh offset for tracking the last read message
					await consumer.StoreOffset(ctx.Offset);
				}
			};

			try
			{
				offset = await streamSystem.QueryOffset(consumerReference, streamName);
				consumerConfig.OffsetSpec = new OffsetTypeOffset(offset + 1); // the next messaeges after the stored offset after the consumer get connected to the stream
			}
			catch (OffsetNotFoundException)
			{
				Console.WriteLine($"there is no stored offset");
			}

			var consumer = await Consumer.Create(consumerConfig);

			Console.WriteLine("Consumer created, now subscribing to stream");
			Console.ReadLine();

			await consumer.Close();
			await streamSystem.Close();

		}

	}
}
