using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesDeadLetterExchangeRoutingService
{
    public static async Task ConumeMessages_dead_letter_exchange_routing()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };

        var connection = await factory.CreateConnectionAsync();
        var channel = await connection.CreateChannelAsync();

        var consumer = new AsyncEventingBasicConsumer(channel);


        consumer.ReceivedAsync += async (model, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());

            Console.WriteLine($"{message} received");

            await channel.BasicRejectAsync(
                deliveryTag: ea.DeliveryTag,
                requeue: false
            );

            Console.WriteLine($"{message} rejected → DLX");
        };

        await channel.BasicConsumeAsync(
            queue: "message-dead-queue",
            autoAck: false,
            consumer: consumer
        );

        Console.WriteLine("Consumer running... press ENTER to exit");
        Console.ReadLine();

        await channel.CloseAsync();
        await connection.CloseAsync();
    }
}
