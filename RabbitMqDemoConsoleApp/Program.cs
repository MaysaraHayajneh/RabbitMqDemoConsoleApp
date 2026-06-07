

using RabbitMQ.Client;
using System.Text;


await Generate_Log__message_topic_permission();
async Task Generate_Log_TopicExchange()
{
    var factory = new ConnectionFactory() { HostName = "localhost" };
    using var connection = await factory.CreateConnectionAsync();

    using (var channel = await connection.CreateChannelAsync())
    {
        for (int i = 0; i < 1000; i++)
        {
            if (i % 2 == 0)
            {
                Thread.Sleep(2000);
                var message = $"log info {i}";
                var body = Encoding.UTF8.GetBytes(message);

                await channel.BasicPublishAsync(
                    exchange: "amq.topic",
                    routingKey: "log.info",
                    body: body
                    );
            }
            else
            {
                Thread.Sleep(2000);
                var message = $"log info {i}";
                var body = Encoding.UTF8.GetBytes(message);

                await channel.BasicPublishAsync(
                    exchange: "amq.topic",
                    routingKey: "log.error",
                    body: body
                    );

            }

        }

        Console.WriteLine("press enter to exist");
        Console.ReadLine();
    }
}
async Task Generate_Log__auto_deleted_queue()
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
                queue: "autodeleted-queue",
                durable: true,
                exclusive: false,
                autoDelete: true
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
async Task Generate_Log__expier_queue()
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

async Task Generate_Log__message_ttl_queue()
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
                queue: "message-ttl-queue",
                durable: true,
                exclusive: false,
                autoDelete: false,
                       arguments: new Dictionary<string, object>
                        {
                            { "x-message-ttl", 10000 }
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

async Task Generate_Log__message_dead_queue_letter_exchaneg()
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

async Task Generate_Log__message_dead_letter_routing_key()
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
                            { "x-dead-letter-exchange", "routed-exchange-direct" },
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

async Task Generate_Log__message_mandatory_false()
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
                            { "x-dead-letter-exchange", "routed-exchange-direct" },
                }
        );
        channel.BasicReturnAsync += async (sender, ea) =>
        {
            var message = Encoding.UTF8.GetString(ea.Body.ToArray());

            Console.WriteLine("Returned message:");
            Console.WriteLine(message);
        };
        string queueName = queue.QueueName;
        for (int i = 0; i < 10; i++)
        {

            Thread.Sleep(2000);
            var message = $"log info {i}";
            var body = Encoding.UTF8.GetBytes(message);




            await channel.BasicPublishAsync(
                exchange: "",
                routingKey: "fsafsafsf",
                body: body,
                mandatory: true
                );


        }

        Console.WriteLine("press enter to exist");
        Console.ReadLine();
    }
}



/// => this is an eexample of refusing writing to the exchange becasue the user dod not have a permission with using keyy log.error for publishong/writing
async Task Generate_Log__message_topic_permission() 
{
    var factory = new ConnectionFactory() { HostName = "localhost", VirtualHost = "dev", UserName = "maysara", Password = "maysara" };

    using var connection = await factory.CreateConnectionAsync();

    using (var channel = await connection.CreateChannelAsync(new CreateChannelOptions(true,true)))
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




