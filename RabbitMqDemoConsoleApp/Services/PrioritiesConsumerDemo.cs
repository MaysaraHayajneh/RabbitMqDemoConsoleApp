using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class PrioritiesConsumerDemo
{
    public static async Task Generate_Log_message_priorities_consumer()
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
}
