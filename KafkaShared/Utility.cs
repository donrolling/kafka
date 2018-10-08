using KafkaNet;
using KafkaNet.Model;
using KafkaNet.Protocol;
using System;
using System.Collections.Generic;

namespace KafkaShared {
	public class Utility {
		public const string Topic = "kafka-test-topic";
		public const string Url = "http://localhost:9092";

		public static Consumer GetConsumer(string topic) {
			var uri = new Uri(Url);
			var options = new KafkaOptions(uri);
			var router = new BrokerRouter(options);
			var consumer = new Consumer(new ConsumerOptions(topic, router));
			return consumer;
		}

		public static Producer GetProducerClient() {
			var uri = new Uri(Url);
			var options = new KafkaOptions(uri);
			var router = new BrokerRouter(options);
			var client = new Producer(router);
			return client;
		}

		public static void SendMessage(string topic, string command) {
			var msg = new Message(command);
			var client = GetProducerClient();
			client.SendMessageAsync(Topic, new List<Message> { msg }).Wait();
		}
	}
}