using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesConsumerPrefetchService
{
    public static async Task ConumeMessages_consumer_prefetch()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };

        var connection = await factory.CreateConnectionAsync();

        var channel = await connection.CreateChannelAsync();

        await channel.BasicQosAsync(0, 2, false);// teliing the raabit mq to send at max two unacked messages // the consumer shoud acked so he can recive further
                                                 // messages
        for (int i = 0; i < 3; i++)
        {

            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                Console.WriteLine($"{message} received");

                //await channel.BasicRejectAsync(
                //	deliveryTag: ea.DeliveryTag,
                //	requeue: false
                //);

                Console.WriteLine($"{message} rejected → DLX");
            };

            await channel.BasicConsumeAsync(
                queue: "message--qudeadeue",
                autoAck: false,
                consumer: consumer
            );
        }


        Console.WriteLine("Consumer running... press ENTER to exit");
        Console.ReadLine();

        await channel.CloseAsync();
        await connection.CloseAsync();
    }
}
