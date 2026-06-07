using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class MaxLengthQueueDemo
{
    public static async Task Generate_Log_message_max_length_queue()
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
}
