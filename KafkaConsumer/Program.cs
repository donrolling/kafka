using KafkaNet;
using KafkaNet.Model;
using System;
using System.Text;

namespace KafkaConsumer {
	internal class Program {

		private static void Main(string[] args) {
			Consumer consumer = KafkaShared.Utility.GetConsumer(KafkaShared.Utility.Topic);
			foreach (var message in consumer.Consume()) {
				Console.WriteLine(Encoding.UTF8.GetString(message.Value));
			}
			Console.ReadLine();
		}
	}
}