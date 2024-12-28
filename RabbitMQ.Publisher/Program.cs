using RabbitMQ.Client;
using System.Text;

await HeaderExchange();

async Task BasicPublisher()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    // queue olusturma
    // exclusive: tum icerik tuketildiginde kuyrugun silinip silinmemesini belirttigimiz kisim
    // durable: mesajinlarin kalici mi bilgisini belirttigimiz kisim
    await channel.QueueDeclareAsync(queue: "example-queue", exclusive: false, autoDelete: false, durable: true);



    // queue'ya mesaj gonderilmesi
    //for (int i = 0; i < 100; i++)
    //{
    //    var message = Encoding.UTF8.GetBytes("merhaba " + i);
    //    await channel.BasicPublishAsync(exchange: "", routingKey: "example-queue", body: message);
    //}

    Console.WriteLine("mesaj gonderildi");
    Console.Read();
}

// mesajlarin islenmeden silinmemesi
async Task DurabilityPublisher()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    // queue olusturma
    // exclusive: tum icerik tuketildiginde kuyrugun silinip silinmemesini belirttigimiz kisim
    // durable: mesajinlarin kalici mi bilgisini belirttigimiz kisim - ayrica mesaj icinde bu tanimi yapmak lazim
    await channel.QueueDeclareAsync(queue: "example-queue", exclusive: false, autoDelete: false, durable: true);

    var properties = new BasicProperties()
    {
        Persistent = true,
    };

    // queue'ya mesaj gonderilmesi
    for (int i = 0; i < 100; i++)
    {
        var message = Encoding.UTF8.GetBytes("merhaba " + i);
        await channel.BasicPublishAsync(exchange: "", routingKey: "example-queue", body: message, basicProperties: properties, mandatory: true);
    }

    Console.WriteLine("mesaj gonderildi");
    Console.Read();
}

// Direct Exchange
// exchange bagli kuyruktan ilgili routinge gore tum consumerlara mesaj iletme
async Task DirectExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(exchange: "direct-exchange-example", type: ExchangeType.Direct);

    while (true)
    {
        Console.Write("Mesaj: ");
        string message = Console.ReadLine();
        var byteMessage = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(
            exchange: "direct-exchange-example",
            routingKey: "direct-queue-example",
            body: byteMessage);

        Console.WriteLine("mesaj gonderildi");
    }
}

// Fanout Exchange
// exchange e bagli tum kuyruklara mesaj gonderme
async Task FanoutExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(
        exchange: "fanout-exchange-example", 
        type: ExchangeType.Fanout);

    while (true)
    {
        Console.Write("Mesaj: ");
        string message = Console.ReadLine();
        var byteMessage = Encoding.UTF8.GetBytes(message);

        await channel.BasicPublishAsync(
            exchange: "fanout-exchange-example",
            routingKey: string.Empty,
            body: byteMessage);

        Console.WriteLine("mesaj gonderildi");
    }
}

// Topic Exchange
// gonderilen formatla eslesen tum kuyruklara mesaj gonderme
// * tek bir kelime
// # birden fazla kelime
// *.log.# logdan once bir kelime logdan sonra istenildigi kadar alan gelebilir
async Task TopicExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(
        exchange: "topic-exchange-example",
        type: ExchangeType.Topic);

    while (true)
    {
        Console.Write("topic belirtiniz: ");
        string topic = Console.ReadLine();
        var byteMessage = Encoding.UTF8.GetBytes("hello world");

        await channel.BasicPublishAsync(
            exchange: "topic-exchange-example",
            routingKey: topic,
            body: byteMessage);

        Console.WriteLine("mesaj gonderildi");
    }
}

// Header Exchange
async Task HeaderExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(
        exchange: "topic-exchange-example",
        type: ExchangeType.Topic);

    while (true)
    {
        Console.Write("topic belirtiniz: ");
        string topic = Console.ReadLine();
        var byteMessage = Encoding.UTF8.GetBytes("hello world");

        await channel.BasicPublishAsync(
            exchange: "topic-exchange-example",
            routingKey: topic,
            body: byteMessage);

        Console.WriteLine("mesaj gonderildi");
    }
}