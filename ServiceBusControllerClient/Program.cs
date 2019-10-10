﻿using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
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

            var tc = new TopicClient( cxn1, "control");



            var entities = new List<dynamic>();
            //entities.Add(new { type = "queue", name = "lh" });
            //entities.Add(new { type = "queue", name = "dl" });
            //entities.Add(new { type = "queue", name = "aa" });
            //entities.Add(new { type = "topic", name = "c10" });
            //entities.Add(new { type = "topic", name = "c11" });
            //entities.Add(new { type = "topic", name = "c12" });
            //entities.Add(new { type = "topic", name = "c13" });
            entities.Add(new { type = "link", topic = "c20", name = "dl" });
            entities.Add(new { type = "link", topic = "c21", name = "dl" });
            entities.Add(new { type = "link", topic = "c22", name = "dl" });


            foreach (dynamic entity in entities)
            {
                await tc.SendAsync(
                    new Message(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(entity)))
                    {
                        Label = "create"
                    });
            }

        }
    }
}
