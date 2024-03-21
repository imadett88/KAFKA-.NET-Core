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

        // Constructeur qui initialise la configuration du consommateur Kafka
        public KafkaConsumerController()
        {
            // Configurer le consommateur Kafka avec le serveur bootstrap, l'ID du groupe et la réinitialisation de l'offset
            _config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "my_consumer_group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };
        }

        // Méthode HTTP GET pour consommer un message
        [HttpGet]
        public IActionResult ConsumeMessage()
        {
            // Créer un consommateur Kafka en utilisant les paramètres configurés
            using (var consumer = new ConsumerBuilder<Ignore, string>(_config).Build())
            {
                consumer.Subscribe("my_topic"); // S'abonner au sujet spécifié

                // Consommer continuellement des messages
                while (true)
                {
                    try
                    {
                        // Tenter de consommer un message dans un délai spécifié
                        var message = consumer.Consume(TimeSpan.FromSeconds(1));
                        if (message != null)
                        {
                            return Ok(message.Message.Value); // Retourner le message s'il est reçu
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Une erreur s'est produite : {e.Error.Reason}");
                    }
                }
            }
        }
    }
}
