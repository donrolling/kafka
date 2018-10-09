using Confluent.Kafka;
using System;
using System.Text;
using System.Threading;

namespace KafkaConsumer {
	internal class Program {

		private static void Main(string[] args) {
			Console.WriteLine($"Waiting for new messages.");
			var consumer = KafkaShared.Utility.GetConsumer(KafkaShared.Utility.Topic, (Message<Null, string> msg) => {
				Console.WriteLine($"Message received: { msg.Value }");
				Console.WriteLine($"Waiting for new messages.");
			});
			Console.WriteLine("Ctrl-C to exit.");
			// Handle Cancel Keypress 
			var cancelled = false;
			Console.CancelKeyPress += (_, e) => {
				e.Cancel = true; // prevent the process from terminating.
				cancelled = true;
			};
			// Poll for messages
			while (!cancelled) {
				consumer.Poll(-1);
				Thread.Sleep(1000);
				Console.WriteLine("Waiting for new message...");
			}
		}
	}
}