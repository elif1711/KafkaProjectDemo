using Confluent.Kafka;
using Microsoft.OpenApi.Extensions;
using System.Diagnostics;
using System.Text.Json;

namespace KafkaProject.Consumer
{
    public class ApacheKafkaConsumerService : IHostedService
    {
        private readonly string topic = "test";
        private readonly string groupId = "test_group";
        private readonly string bootstrapServers = "localhost:9092";
        public Task StartAsync(CancellationToken cancellationToken)
        {
            var config = new ConsumerConfig
            {
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            try
            {
                using (var consumerBuilder = new ConsumerBuilder
                <string, string>(config).Build())
                {
                    consumerBuilder.Subscribe(topic);
                    var cancelToken = new CancellationTokenSource();

                    try
                    {
                        while (true)
                        {
                            var consumer = consumerBuilder.Consume
                               (cancelToken.Token);

                            var messageType = consumer.Key;
                            switch (messageType)
                            {
                                case nameof(MessageType.AddedBasketRequest):
                                    AddedBasketRequest req1 = JsonSerializer.Deserialize<AddedBasketRequest>(consumer.Message.Value);
                                    break;
                                case nameof(MessageType.OrderRequest):
                                    OrderProcessingRequest req = JsonSerializer.Deserialize<OrderProcessingRequest>(consumer.Message.Value);
                                    break;
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        consumerBuilder.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex.Message);
            }

            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }
    }
}
