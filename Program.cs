using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KeyProbe;
using MassTransit;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("MT_LICENSE")))
{
    Console.Error.WriteLine("Set MT_LICENSE to your own MassTransit license before running.");
    return 2;
}

var broker = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS") ?? "localhost:29092";
var database = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION")
    ?? "Host=localhost;Port=25432;Database=repro;Username=postgres";
var runId = Guid.NewGuid().ToString("N");
var topic = $"key-probe-{runId}";
const string key = "sample-key";
using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
var cancellationToken = timeout.Token;

try
{
    using (var admin = new AdminClientBuilder(new AdminClientConfig
    {
        BootstrapServers = broker,
        SocketTimeoutMs = 10000
    }).Build())
    {
        await admin.CreateTopicsAsync(
            [new TopicSpecification { Name = topic, NumPartitions = 1, ReplicationFactor = 1 }],
            new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) });
    }

    var builder = Host.CreateApplicationBuilder();
    builder.Logging.ClearProviders();
    builder.Logging.AddSimpleConsole(options => options.SingleLine = true);
    builder.Logging.SetMinimumLevel(LogLevel.Warning);
    builder.Services.AddDbContext<ProbeDbContext>(options => options.UseNpgsql(database));
    builder.Services.Configure<MassTransitHostOptions>(options =>
    {
        options.WaitUntilStarted = true;
        options.StartTimeout = TimeSpan.FromSeconds(20);
        options.StopTimeout = TimeSpan.FromSeconds(10);
    });
    builder.Services.AddMassTransit(bus =>
    {
        bus.UsingInMemory();
        bus.AddEntityFrameworkOutbox<ProbeDbContext>(outbox =>
        {
            outbox.UsePostgres();
            outbox.QueryDelay = TimeSpan.FromSeconds(1);
            outbox.UseBusOutbox();
        });
        bus.AddRider(rider =>
        {
            rider.AddProducer<string, ProbeRecord>(topic, (context, producer) =>
            {
                producer.SetKeySerializer(Serializers.Utf8);
                producer.SetValueSerializer(new ProbeSerializer());
                producer.EnableIdempotence = true;
                producer.MessageTimeout = TimeSpan.FromSeconds(10);
            });
            rider.UsingKafka((context, kafka) => kafka.Host(broker));
        });
    });

    using var host = builder.Build();
    await using (var scope = host.Services.CreateAsyncScope())
    {
        await scope.ServiceProvider.GetRequiredService<ProbeDbContext>()
            .Database.EnsureCreatedAsync(cancellationToken);
    }

    using var consumer = new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
    {
        BootstrapServers = broker,
        GroupId = $"key-observer-{runId}",
        EnableAutoCommit = false,
        AutoOffsetReset = AutoOffsetReset.Earliest
    }).SetKeyDeserializer(Deserializers.ByteArray)
      .SetValueDeserializer(Deserializers.ByteArray)
      .Build();
    consumer.Assign(new TopicPartitionOffset(topic, new Partition(0), Offset.Beginning));

    await host.StartAsync(cancellationToken);
    try
    {
        var direct = host.Services.GetRequiredService<IKafkaRider>()
            .GetProducer<string, ProbeRecord>(new Uri($"topic:{topic}"));
        await direct.Produce(key, new ProbeRecord(runId, "direct"), cancellationToken);

        await using (var scope = host.Services.CreateAsyncScope())
        {
            var producer = scope.ServiceProvider.GetRequiredService<ITopicProducer<string, ProbeRecord>>();
            var db = scope.ServiceProvider.GetRequiredService<ProbeDbContext>();
            await producer.Produce(key, new ProbeRecord(runId, "outbox"), cancellationToken);
            await db.SaveChangesAsync(cancellationToken);
        }

        var observed = new Dictionary<string, byte[]?>();
        while (observed.Count < 2)
        {
            var result = consumer.Consume(cancellationToken);
            var record = JsonSerializer.Deserialize<ProbeRecord>(result.Message.Value)
                ?? throw new InvalidOperationException("Received a null record.");
            if (record.RunId != runId || record.Case is not ("direct" or "outbox"))
                throw new InvalidOperationException("Received an unexpected record.");
            if (!observed.TryAdd(record.Case, result.Message.Key))
                throw new InvalidOperationException("Received a duplicate case.");
            var actualKey = result.Message.Key is null ? "<null>" : Convert.ToHexString(result.Message.Key);
            Console.WriteLine($"{record.Case}: key bytes={actualKey}; partition={result.Partition.Value}; offset={result.Offset.Value}");
        }

        var expectedKey = Encoding.UTF8.GetBytes(key);
        if (observed["direct"] is not { } directKey || !directKey.AsSpan().SequenceEqual(expectedKey))
            throw new InvalidOperationException("Direct control did not preserve the expected UTF-8 key.");
        if (observed["outbox"] is null)
        {
            Console.Error.WriteLine("REPRODUCED: the direct key is correct, but the EF bus outbox Kafka key is null.");
            return 1;
        }
        if (!observed["outbox"]!.AsSpan().SequenceEqual(expectedKey))
            throw new InvalidOperationException("Outbox key changed, but was not null; investigate separately.");

        Console.WriteLine("NOT REPRODUCED: both Kafka records preserved the expected UTF-8 key.");
        return 0;
    }
    finally
    {
        consumer.Close();
        using var stopTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await host.StopAsync(stopTimeout.Token);
    }
}
catch (Exception exception)
{
    // Do not print exception messages, which may contain connection details.
    Console.Error.WriteLine($"INCONCLUSIVE: {exception.GetType().Name}; check local infrastructure/configuration.");
    return 2;
}

namespace KeyProbe
{
    public sealed record ProbeRecord(string RunId, string Case);

    public sealed class ProbeSerializer : ISerializer<ProbeRecord>
    {
        public byte[] Serialize(ProbeRecord data, SerializationContext context) =>
            JsonSerializer.SerializeToUtf8Bytes(data);
    }

    public sealed class ProbeDbContext(DbContextOptions<ProbeDbContext> options) : DbContext(options)
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.AddTransactionalOutboxEntities();
    }
}
