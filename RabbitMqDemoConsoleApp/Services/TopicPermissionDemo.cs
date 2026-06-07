using RabbitMQ.Client;
using System.Text;

namespace RabbitMqDemoConsoleApp.Services;

public static class TopicPermissionDemo
{
    /// => this is an eexample of refusing writing to the exchange becasue the user dod not have a permission with using keyy log.error for publishong/writing
    public static async Task Generate_Log__message_topic_permission()
    {
        var factory = new ConnectionFactory() { HostName = "localhost", VirtualHost = "dev", UserName = "maysara", Password = "maysara" };

        using var connection = await factory.CreateConnectionAsync();

        using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true, true)))
        {

            channel.BasicReturnAsync += async (sender, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                Console.WriteLine("Returned message:");
                Console.WriteLine(message);
            };

            channel.BasicAcksAsync += async (sender, ea) =>
            {
                var message = Encoding.UTF8.GetString(BitConverter.GetBytes(ea.DeliveryTag));

                Console.WriteLine("Acked message:");
                Console.WriteLine(message);
            };
            for (int i = 0; i < 10; i++)
            {

                Thread.Sleep(2000);
                var message = $"log info {i}";
                var body = Encoding.UTF8.GetBytes(message);

                await channel.BasicPublishAsync(
                    exchange: "amq.topic",
                    routingKey: "log.error",
                    body: body,
                    mandatory: true
                    );

            }

            Console.WriteLine("press enter to exist");
            Console.ReadLine();
        }
    }
}
