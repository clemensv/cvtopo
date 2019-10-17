using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusControllerClient
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
        }

        static async Task MainAsync(string[] args)
        {
            var cxn = "Endpoint=sb://cvtopoweu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=ibKoXIg8F6SYQVuInImVGYqU+qva3e96AV4gTgtCk9k=";
            var cxn1 = "Endpoint=sb://cvtoponeu.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=J5PVXWKtL4/ya7u+nYKWkZqyycvHdj9FYVfhzT7K77I=";

            //var tc = new TopicClient( cxn1, "control");



            //var entities = new List<dynamic>();
            //entities.Add(new { type = "queue", name = "lh" });
            //entities.Add(new { type = "queue", name = "dl" });
            //entities.Add(new { type = "queue", name = "aa" });
            //entities.Add(new { type = "topic", name = "c10" });
            //entities.Add(new { type = "topic", name = "c11" });
            //entities.Add(new { type = "topic", name = "c12" });
            //entities.Add(new { type = "topic", name = "c13" });
            //entities.Add(new { type = "link", topic = "c23", name = "dl" });
            //entities.Add(new { type = "link", topic = "c24", name = "dl" });
            //entities.Add(new { type = "link", topic = "c25", name = "dl" });


            //foreach (dynamic entity in entities)
            //{
            //    await tc.SendAsync(
            //        new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entity)))
            //        {
            //            Label = "create"
            //        });
            //}


            var latencyTracker = new Dictionary<string, Stopwatch>();
            var lufthansaW = new QueueClient(cxn1, "lh");
            lufthansaW.RegisterMessageHandler(async (m, c) => {
                if (latencyTracker.TryGetValue(m.MessageId, out var sw))
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine($"{m.Label}:{m.MessageId}:{sw.ElapsedMilliseconds}ms");
                }
                else
                {
                    Console.WriteLine(m.MessageId);
                }
            }, async e => { });

            var lufthansaN = new QueueClient(cxn, "lh");
            lufthansaN.RegisterMessageHandler(async (m, c) => {
                if (latencyTracker.TryGetValue(m.MessageId, out var sw))
                {
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    Console.WriteLine($"{m.Label}:{m.MessageId}:{sw.ElapsedMilliseconds}ms");
                }
                else
                {
                    Console.WriteLine(m.MessageId);
                }
            }, async e => { });


            var t1 = Task.Run(async () =>
            {
                var gateA10 = new TopicClient(cxn, "a10");
                for (int i = 0; i < 120; i++)
                {
                    var id = Guid.NewGuid().ToString();
                    var sw = new Stopwatch();
                    Message swipe = new Message()
                    {
                        MessageId = id,
                        Label = "LH490",
                        TimeToLive = TimeSpan.FromSeconds(10)
                    };
                    latencyTracker.Add(id, sw);
                    sw.Start();
                    await gateA10.SendAsync(swipe);
                    await Task.Delay(500);
                }
            });

            var t2 = Task.Run(async () =>
            {
                var gateA11 = new TopicClient(cxn, "a11");
                for (int i = 0; i < 120; i++)
                {
                    var id = Guid.NewGuid().ToString();
                    var sw = new Stopwatch();
                    Message swipe = new Message()
                    {
                        MessageId = id,
                        Label = "LH2004",
                        TimeToLive = TimeSpan.FromSeconds(10)
                    };
                    latencyTracker.Add(id, sw);
                    sw.Start();
                    await gateA11.SendAsync(swipe);
                    await Task.Delay(500);
                }
            });

            var t3 = Task.Run(async () =>
            {
                var gateA12 = new TopicClient(cxn, "a12");
                for (int i = 0; i < 120; i++)
                {
                    var id = Guid.NewGuid().ToString();
                    var sw = new Stopwatch();
                    Message swipe = new Message()
                    {
                        MessageId = id,
                        Label = "LH440",
                        TimeToLive = TimeSpan.FromSeconds(10)
                    };
                    latencyTracker.Add(id, sw);
                    sw.Start();
                    await gateA12.SendAsync(swipe);
                    await Task.Delay(500);
                }
            });
            await Task.WhenAll(t1, t2, t3);

            Console.ReadLine();

        }
    }
}
