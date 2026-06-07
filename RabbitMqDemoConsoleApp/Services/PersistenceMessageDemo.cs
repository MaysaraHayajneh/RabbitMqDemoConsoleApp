using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class PersistenceMessageDemo
{
    public static async Task Generate_persitence_message()
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
}
