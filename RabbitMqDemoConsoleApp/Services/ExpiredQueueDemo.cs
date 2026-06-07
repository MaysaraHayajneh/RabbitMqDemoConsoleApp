using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class ExpiredQueueDemo
{
    public static async Task Generate_Log__expier_queue()
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
}
