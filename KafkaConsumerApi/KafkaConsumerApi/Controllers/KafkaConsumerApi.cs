using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Threading;

namespace KafkaConsumerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaConsumerController : ControllerBase
    {
        private readonly ConsumerConfig _config;

        public KafkaConsumerController()
        {
            _config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "demo_group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        [HttpGet]
        public IActionResult ConsumeMessage()
        {
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe("demo");

                while (true)
                {
                    try
                    {
                        var message = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (message != null)
                        {
                            return Ok(message.Message.Value);
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occurred: {e.Error.Reason}");
                    }
                }
            }
        }
    }
}
