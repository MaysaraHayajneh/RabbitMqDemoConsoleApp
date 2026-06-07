


using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using System.Buffers;
using System.Net;
using System.Text;

await ReadFromStream();

async Task ReadFromStream()
{


    var config = new StreamSystemConfig()
    {
        VirtualHost = "dev",
        Endpoints = new List<EndPoint> { new IPEndPoint(IPAddress.Loopback, 5552) }, // rabbit ,q stream port
        UserName = "maysara",
        Password = "maysara"
    };


    var streamSystem = await StreamSystem.Create(config);

    const string streamName = "stream-q";

    var consumerConfig = new ConsumerConfig(streamSystem, streamName)
    {
        Reference = "DOTNET 10",

        //OffsetSpec = new OffsetTypeOffset(), if this did not get determinde will read from the next by default



        // recive teh message event 
        MessageHandler = async (sourceStream, consumer, ctx, message) =>
        {
            Console.WriteLine($"message coming form {sourceStream} daata :{Encoding.Default.GetString(message.Data.Contents.ToArray())}  consumed {consumer.Info.Identifier}");
        }
    };

    var consumer = await Consumer.Create(consumerConfig);

    Console.WriteLine("Consumer created, now subscribing to stream");
    Console.ReadLine();

    await consumer.Close();
    await streamSystem.Close();

}
