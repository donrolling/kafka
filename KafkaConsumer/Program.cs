using Confluent.Kafka;
using System;
using System.Text;
using System.Threading;

namespace KafkaConsumer {
	internal class Program {

		private static void Main(string[] args) {
			var consumer = KafkaShared.Utility.GetConsumer(KafkaShared.Utility.Topic, (Message<Null, string> msg) => {
				Console.WriteLine($"Message received: { msg.Value }");
				Console.WriteLine($"Waiting for new messages.");
			});
			Console.WriteLine("Ctrl-C to exit.\r\n");
			Console.WriteLine($"Waiting for messages.");
			// Handle Cancel Keypress 
			var cancelled = false;
			Console.CancelKeyPress += (_, e) => {
				e.Cancel = true; // prevent the process from terminating.
				cancelled = true;
			};
			// Poll for messages
			while (!cancelled) {
				consumer.Poll(-1);
			}
		}
	}
}