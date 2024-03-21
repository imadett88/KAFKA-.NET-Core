using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace KafkaProducerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig _config;


        public KafkaProducerController()
        {
            _config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }

        [HttpPost]
        public async Task<IActionResult> ProduceMessage([FromBody] string msg)
        {
            using (var producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                await producer.ProduceAsync("demo",new Message<Null, string> { Value = msg });
                return Ok();
            }
        }
    }
}
