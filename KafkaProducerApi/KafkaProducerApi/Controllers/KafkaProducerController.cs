using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Threading.Tasks;

namespace KafkaProducerApi.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class KafkaProducerController : ControllerBase
    {
        private readonly ProducerConfig _config;

        // Constructeur qui initialise la configuration du producteur Kafka
        public KafkaProducerController()
        {
            // Configurer le producteur Kafka avec le serveur bootstrap
            _config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
            };
        }

        // Méthode HTTP POST pour produire un message
        [HttpPost]
        public async Task<IActionResult> ProduceMessage([FromBody] string message)
        {
            // Créer un producteur Kafka en utilisant les paramètres configurés
            using (var producer = new ProducerBuilder<Null, string>(_config).Build())
            {
                // Produire un message de façon asynchrone sur le sujet spécifié
                await producer.ProduceAsync("my_topic", new Message<Null, string> { Value = message });
                return Ok(); // Retourner 200 OK si le message est produit avec succès
            }
        }
    }
}
