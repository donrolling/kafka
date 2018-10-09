using System;
using System.Collections.Generic;

namespace KafkaProducer {
	internal class Program {

		private static void Main(string[] args) {
			var command = string.Empty;
			while (command != "exit") {
				Console.WriteLine("Type 'exit' to quit or type any message to send it.");
				command = Console.ReadLine();
				if (command == "exit") {
					continue;
				}
				KafkaShared.Utility.SendMessage(KafkaShared.Utility.Topic, command);
			}
		}
	}
}