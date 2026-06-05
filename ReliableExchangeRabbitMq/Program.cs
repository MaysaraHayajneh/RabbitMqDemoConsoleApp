

using RabbitMQ.Client;
await Publish();
async Task Publish()
{
    var factory = new ConnectionFactory() { HostName = "localhost" };

    using (var connection = await factory.CreateConnectionAsync())
    {
        using (var channel = await connection.CreateChannelAsync())
        {

            channel.BasicReturnAsync += async (sender, ea) =>
            {

                Console.WriteLine($"Message with delivery tag {ea.Exchange} has been returned.");
                await Task.CompletedTask;

            };
            string msg = "hellow world";

            await channel.BasicPublishAsync(
                exchange: "reliable-exchange",
                routingKey: "reliable-routing-key",
                body: System.Text.Encoding.UTF8.GetBytes(msg),
                mandatory: true
                );

        }
    }

}

Console.ReadLine();