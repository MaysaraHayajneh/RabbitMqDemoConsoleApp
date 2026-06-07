using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesTopicPermissionReadService
{
    public static async Task ConumeMessages_topic_permission_read()
    {
        var factory = new ConnectionFactory() { HostName = "localhost", VirtualHost = "dev", UserName = "maysara", Password = "maysara" };

        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        await channel.QueueBindAsync(
            queue: "q1",
            exchange: "amq.topic",
            routingKey: "log.error"
        );

        var consumer = new AsyncEventingBasicConsumer(channel);

        consumer.ReceivedAsync += async (model, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());

            Console.WriteLine($"{message} received");

            await channel.BasicAckAsync(
                deliveryTag: ea.DeliveryTag,
                multiple: false
            );

            Console.WriteLine($"{message} acknowledged");
        };

        await channel.BasicConsumeAsync(
            queue: "q1",
            autoAck: false,
            consumer: consumer
        );

        Console.WriteLine("Consumer running... press ENTER to exit");
        Console.ReadLine();

        await channel.CloseAsync();
        await connection.CloseAsync();
    }
}
