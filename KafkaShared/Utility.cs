using Confluent.Kafka.Serialization; 
using Confluent.Kafka; 
using System; 
using System.Collections.Generic; 
using System.Text; 

namespace KafkaShared {
	public class Utility {
		public const string Topic = "testtopic"; 
		private const string _groupIdKey = "group.id"; 
		private const string _groupId = "myconsumer"; 
		private const string _bootstrapServerKey = "bootstrap.servers"; 
		private const string _bootstrapServerUrl = "127.0.0.1:9092"; 

		public static Consumer<Null, string> GetConsumer(string topic, Action<Message<Null, string>> action) {
			var consumerConfig = new Dictionary <string, object>  { {_groupIdKey, _groupId },  {_bootstrapServerKey, _bootstrapServerUrl }, 
            };
			var consumer = new Consumer<Null, string> (consumerConfig, null, new StringDeserializer(Encoding.UTF8)); 
			
            //Subscribe to the OnMessage event
            consumer.OnMessage += (obj, msg) => {
				action.Invoke(msg);
			};

            //Subscribe to the topics
			var topics = new List<string>{ topic }; 
			consumer.Subscribe(topics); 
			
			return consumer;
		}

		private static Producer < Null, string > getProducerClient() {			
            var producerConfig = new Dictionary < string, object >  { {_bootstrapServerKey, _bootstrapServerUrl }}; 
            return new Producer < Null, string > (producerConfig, null, new StringSerializer(Encoding.UTF8)); 
		}

		public static void SendMessage(string topic, string message) {
			var producer = getProducerClient(); 
			var result = producer.ProduceAsync(topic, null, message).GetAwaiter().GetResult(); 
		}
	}
}