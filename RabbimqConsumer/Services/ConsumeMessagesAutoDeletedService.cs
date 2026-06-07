using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesAutoDeletedService
{
    public static async Task ConumeMessages_auto_deleted()
    {
        var factory = new ConnectionFactory() { HostName = "localhost" };
        using (var connection = await factory.CreateConnectionAsync())
        {
            using (var channel = await connection.CreateChannelAsync())
            {
                var consumer = new AsyncEventingBasicConsumer(channel);
                consumer.ReceivedAsync += async (model, ea) =>
                {
                    var body = ea.Body.ToArray();
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine($"{message} has been cosumed by {ea.ConsumerTag}");
                    Console.WriteLine($"===================================================");

                    await channel.BasicAckAsync(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false
                        );
                };

                await channel.BasicConsumeAsync(
                    queue: "autodeleted-queue",
                    autoAck: false,
                    consumer: consumer
                    );


                Console.WriteLine("press enter to exit");
                Console.ReadKey();
            }
        }
    }
}
