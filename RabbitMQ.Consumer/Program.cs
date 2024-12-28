using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Text;


await TopicExchange();

static async Task BasicConsumer()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    // queue olusturma
    // consumer'daki tanimlama publisher ile ayni olmalidir
    var queueName = "example-queue";
    await channel.QueueDeclareAsync(queue: queueName, exclusive: false, autoDelete: false, durable: true);
    // ayni anda kac mesaji isleyecegi bilgisini tanimladigimiz yer
    // bu ayar yapilmazsa tum mesajlar ilk baglanan consumer a gider 
    await channel.BasicQosAsync(prefetchSize: 0, prefetchCount: 1, global: false);

    // queue'dan mesaj okuma
    AsyncEventingBasicConsumer consumer = new(channel);

    // autoAck: queue'dan alinan mesajin basarili yanitini almadan silinmemesi saglar. Bu ayar consumerda yapilir.
    await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer);

    consumer.ReceivedAsync += async (sender, e) =>
    {
        Console.WriteLine(Encoding.UTF8.GetString(e.Body.Span));

        Task.Delay(1000).Wait();
        // multiple: sadece ilgili mesaja dair onay bilgisini yollar. Aksi takdirde bundan oncekiler icinde onaylar
        await channel.BasicAckAsync(deliveryTag: e.DeliveryTag, multiple: false);

        // islenmeyen mesaji geri gonderme
        // requeue: tekrardan kuyruga eklenmesi isteniyorsa eklenir
        //await channel.BasicNackAsync(deliveryTag: e.DeliveryTag, multiple: false, requeue: true);

    };


    // bir kuyruktaki tum mesajlarin islenmesini reddetme
    //var consumerTag = await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer);
    //await channel.BasicCancelAsync(consumerTag);

    // bir kuyruktaki tek bir mesajin islenmesini reddetme
    //var consumerTag = await channel.BasicConsumeAsync(queue: queueName, autoAck: false, consumer);
    //await channel.BasicRejectAsync(deliveryTag: e.DeliveryTag, requeue: true);

    Console.Read();
}

static async Task DirectExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    // 1.adim
    await channel.ExchangeDeclareAsync(
        exchange: "direct-exchange-example",
        type: ExchangeType.Direct);

    // 2.adim
    var queueName = channel.QueueDeclareAsync().Result.QueueName;

    // 3.adim
    await channel.QueueBindAsync(
        queue: queueName,
        exchange: "direct-exchange-example",
        routingKey: "direct-queue-example");

    AsyncEventingBasicConsumer consumer = new(channel);

    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: true,
        consumer: consumer);

    consumer.ReceivedAsync += async (sender, e) =>
    {
        string message = Encoding.UTF8.GetString(e.Body.Span);
        Console.WriteLine(message);

        Task.Delay(1000).Wait();
    };


    Console.Read();

    // 1.Adim publisher'da ki exchange ile birebir ayni isim ve type'a sahip bir exchange tanimlanmalidir
    // 2.adim publisher tarafindan routing key'de bulunan degerdeki kuyruga gonderilen mesajlari kendi olusturdugumuz kuyruga yonlendirerek tuketmemiz gerekmektedir. Bunun icin oncelikli bir kuyruk olusturulmalidir

}

static async Task FanoutExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(
        exchange: "fanout-exchange-example",
        type: ExchangeType.Fanout);

    Console.Write("Kuyruk adini giriniz: ");
    var queueName = Console.ReadLine();
    await channel.QueueDeclareAsync(
        queue: queueName,
        exclusive: false);

    await channel.QueueBindAsync(
        queue: queueName,
        exchange: "fanout-exchange-example",
        routingKey: string.Empty);

    AsyncEventingBasicConsumer consumer = new(channel);

    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: true, // message acknowledge
        consumer: consumer);

    consumer.ReceivedAsync += async (sender, e) =>
    {
        string message = Encoding.UTF8.GetString(e.Body.Span);
        Console.WriteLine(message);

        Task.Delay(1000).Wait();
    };


    Console.Read();
}

static async Task TopicExchange()
{
    // baglanti olusturma
    ConnectionFactory factory = new();
    factory.Uri = new("amqps://cbnrbnmg:V89R6sjHbaxQ60a35enaOAHUe0QV5Ywe@sparrow.rmq.cloudamqp.com/cbnrbnmg");
    var connection = await factory.CreateConnectionAsync();
    var channel = await connection.CreateChannelAsync();

    await channel.ExchangeDeclareAsync(
        exchange: "topic-exchange-example",
        type: ExchangeType.Topic);

    Console.Write("Dinlenecek topic formatini giriniz: ");
    var topic = Console.ReadLine();
    var queueName = channel.QueueDeclareAsync().Result.QueueName;

    await channel.QueueBindAsync(
        queue: queueName,
        exchange: "topic-exchange-example",
        routingKey: topic);

    AsyncEventingBasicConsumer consumer = new(channel);

    await channel.BasicConsumeAsync(
        queue: queueName,
        autoAck: true, // message acknowledge
        consumer: consumer);

    consumer.ReceivedAsync += async (sender, e) =>
    {
        string message = Encoding.UTF8.GetString(e.Body.Span);
        Console.WriteLine(message);

        Task.Delay(1000).Wait();
    };


    Console.Read();
}