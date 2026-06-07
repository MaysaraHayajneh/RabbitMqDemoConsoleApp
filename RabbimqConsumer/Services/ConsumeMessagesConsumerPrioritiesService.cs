using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesConsumerPrioritiesService
{
    public static async Task ConumeMessages_consumer_priorities()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };

        var connection = await factory.CreateConnectionAsync();

        var channel = await connection.CreateChannelAsync();

        await channel.BasicQosAsync(0, 3, false);// teliing the raabit mq to send at max two unacked messages // the consumer shoud acked so he can recive further
                                                 // messages
        for (int i = 0; i < 3; i++)
        {
            var consumer = new AsyncEventingBasicConsumer(channel);

            consumer.ReceivedAsync += async (model, ea) =>
            {
                var message = Encoding.UTF8.GetString(ea.Body.ToArray());

                Console.WriteLine($"{message} received");

                //await channel.BasicAckAsync(
                //	deliveryTag: ea.DeliveryTag,
                //	false
                //);

                Console.WriteLine($"{message} has been processed by consumer {ea.ConsumerTag}");
            };

            Dictionary<string, object> args = new Dictionary<string, object>();

            if (i == 0)
            {
                args.Add("x-priority", 10);
            }
            else if (i == 1)
            {
                args.Add("x-priority", 1);

            }
            await channel.BasicConsumeAsync(
                queue: "q1",
                autoAck: false,
                consumer: consumer,
                consumerTag: $"consumer{i}",
                arguments: args
            );
        }


        Console.WriteLine("Consumer running... press ENTER to exit");
        Console.ReadLine();

        await channel.CloseAsync();
        await connection.CloseAsync();
    }
}
