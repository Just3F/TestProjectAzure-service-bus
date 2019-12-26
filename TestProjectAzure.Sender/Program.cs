using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;

namespace TestProjectAzure.Sender
{
    class Program
    {
        static ITopicClient topicClient;
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        static async Task MainAsync()
        {
            string ServiceBusConnectionString = "Endpoint=sb://testwesteuropeservicebus.servicebus.windows.net/;SharedAccessKeyName=user-send;SharedAccessKey=bdfzlrnB9jmkr87RVM1zRB6hrUAiAsa+L2ydD6KpZZw=";
            string TopicName = "users-temp-topic";

            topicClient = new TopicClient(ServiceBusConnectionString, TopicName);

            // Send messages.
            await SendUserMessage();

            Console.ReadKey();

            await topicClient.CloseAsync();
        }


        static async Task SendUserMessage()
        {
            List<User> users = GetDummyDataForUser();

            var serializeUser = JsonConvert.SerializeObject(users);

            string messageType = "userData";

            string messageId = Guid.NewGuid().ToString();

            var message = new ServiceBusMessage
            {
                Id = messageId,
                Type = messageType,
                Content = serializeUser
            };

            var serializeBody = JsonConvert.SerializeObject(message);

            // send data to bus

            var busMessage = new Message(Encoding.UTF8.GetBytes(serializeBody));
            busMessage.UserProperties.Add("Type", messageType);
            busMessage.MessageId = messageId;

            await topicClient.SendAsync(busMessage);

            Console.WriteLine($"message byte {busMessage.Size} has been sent 1: " + DateTime.UtcNow.ToString("HH:mm:ss.fff"));

            await Task.Delay(1000);

            await topicClient.SendAsync(busMessage);

            Console.WriteLine($"message byte {busMessage.Size} has been sent 2: " + DateTime.UtcNow.ToString("HH:mm:ss.fff"));

            await Task.Delay(1000);

            await topicClient.SendAsync(busMessage);

            Console.WriteLine($"message byte {busMessage.Size} has been sent 3: " + DateTime.UtcNow.ToString("HH:mm:ss.fff"));

            await Task.Delay(1000);

            await topicClient.SendAsync(busMessage);

            Console.WriteLine($"message byte {busMessage.Size} has been sent 4: " + DateTime.UtcNow.ToString("HH:mm:ss.fff"));

        }

        public class User
        {
            public int Id { get; set; }

            public string Name { get; set; }
        }

        public class ServiceBusMessage
        {
            public string Id { get; set; }
            public string Type { get; set; }
            public string Content { get; set; }
        }

        private static List<User> GetDummyDataForUser()
        {
            List<User> lstUsers = new List<User>();
            for (int i = 1; i < 6000; i++)
            {
                var user = new User { Id = i, Name = "CPVariyani" + i };

                lstUsers.Add(user);
            }

            return lstUsers;
        }
    }
}
