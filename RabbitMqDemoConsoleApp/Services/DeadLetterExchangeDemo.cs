using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class DeadLetterExchangeDemo
{
    public static async Task Generate_Log__message_dead_queue_letter_exchaneg()
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
}
