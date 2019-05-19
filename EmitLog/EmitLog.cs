using RabbitMQ.Client;
using System;
using System.Text;
using System.Timers;
using System.Security.Cryptography;

namespace EmitLog
{
    class EmitLog
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            SetTimer();

            Console.ReadKey();
        }

        private static void SetTimer()
        {
            // Create a timer with a two second interval.
            var messageTimer = new Timer(2000);
            // Hook up the Elapsed event for the timer. 
            messageTimer.Elapsed += OnTimedEvent;
            messageTimer.AutoReset = true;
            messageTimer.Enabled = true;
        }

        private static void OnTimedEvent(Object source, ElapsedEventArgs e)
        {
            var factory = new ConnectionFactory()
            {
                HostName = "192.168.15.33",
                UserName = "renatocolaco",
                Password = "secnet123"
            };

            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(exchange: "direct_logs", type: "direct");
                
                var message = GetMessage();
                string severity = GetSeverity();

                var body = Encoding.UTF8.GetBytes($"[{severity}] {message}");
                                
                channel.BasicPublish(exchange: "direct_logs",
                                     routingKey: severity,
                                     basicProperties: null,
                                     body: body);
            }

            Console.WriteLine("A new message published to the queue at {0:HH:mm:ss.fff}",
                              e.SignalTime);
        }

        /// <summary>
        /// Gets a random severity 
        /// </summary>
        /// <returns></returns>
        private static string GetSeverity()
        {
            var severityArr = new string[] { "info", "warning", "error" };
            using (var randomNumberGenerator = RandomNumberGenerator.Create())
            {
                var randomNumber = new byte[1];
                randomNumberGenerator.GetBytes(randomNumber);
                var rest = randomNumber[0] % severityArr.Length;
                return severityArr[rest];
            }
        }

        private static string GetMessage()
        {
            return $"Hello World! {DateTime.Now:HH:mm:ss}";
        }
    }
}
