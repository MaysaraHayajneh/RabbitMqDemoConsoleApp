using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;

namespace RabbimqConsumer.Services;

public static class ConsumeMessagesService
{
    public static async Task ConumeMessages()
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

                    await channel.BasicAckAsync(  // acknowledgment is a mechanism used in message queuing systems to confirm that a message has been successfully received and processed by a consumer
                                                  // . It helps ensure reliable message delivery and allow // s the message broker to manage the lifecycle of messages effectively (deleted/deleting it).
                        deliveryTag: ea.DeliveryTag,
                        multiple: false
                        );



                    await channel.BasicNackAsync(   // This means i do negative acknowledgment to the message ,  determine if requeue or not  //
                                                    // this will activate the deae-letter exchange freature if teh requeue is false and the message will be sent to the dead-letter exchange if it is configured, otherwise it will be discarded.

                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: false
                        );
                };

                await channel.BasicConsumeAsync(
                    queue: "loginfo",
                    autoAck: false, // has three types of acknowledgment: autoAck, manualAck, and NAck (negative acknowledgment)
                    consumer: consumer,
                    consumerTag: "info"
                    );
                await channel.BasicConsumeAsync(
                    queue: "logerror",
                    autoAck: false, // has three types of acknowledgment: autoAck, manualAck, and NAck (negative acknowledgment)
                    consumer: consumer,
                    consumerTag: "error"
                    );
                await channel.BasicConsumeAsync(
                    queue: "logall",
                    autoAck: false, // has three types of acknowledgment: autoAck, manualAck, and NAck (negative acknowledgment)
                    consumer: consumer,
                    consumerTag: "all"
                    );


                Console.WriteLine("press enter to exit");
                Console.ReadKey();
            }
        }
    }
}
