using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Core;
using Microsoft.Azure.ServiceBus.Management;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace AzureServiceBusController
{
    public class Control
    {
        static object connectionMutex = new object();
        static Dictionary<string, MessageSender> senders = new Dictionary<string, MessageSender>();

        [FunctionName("ControlImport")]
        [return: ServiceBus("%control-topic%", Connection = "control-cxn")]
        public Message ControlImport([ServiceBusTrigger("%remote-control-topic%", "%remote-control-sub%", Connection = "remote-control-cxn")]
                               Message message, ILogger log)
        {
            return message.Clone();
        }

        [FunctionName("Bridge")]
        public async Task Bridge([ServiceBusTrigger("%remote-bridge-queue%", Connection = "remote-control-cxn")]
                               Message message, ILogger log)
        {
            MessageSender sender;
            var msg = message.Clone();
            lock (connectionMutex)
            {
                if (!senders.TryGetValue(message.To, out sender))
                {
                    sender = new MessageSender(Environment.GetEnvironmentVariable("control-cxn"), message.To, 
                        new RetryExponential(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(30), 10));
                    senders.Add(message.To, sender);
                }
            }
            msg.To = null;
            await sender.SendAsync(msg);            
        }

        [FunctionName("ControlHandler")]
        public async Task ControlHandlerAsync([ServiceBusTrigger("%control-topic%", "%control-sub%", Connection = "control-cxn")]
                               Message message, ILogger log)
        {
            if (message.Body == null)
                return;

            ManagementClient mc = new ManagementClient(Environment.GetEnvironmentVariable("control-cxn"));
            switch (message.Label?.ToLowerInvariant())
            {
                case "create":
                    {
                        var create = JObject.Parse(Encoding.UTF8.GetString(message.Body));
                        var type = create.Value<string>("type");
                        var name = create.Value<string>("name");
                        var topic = create.Value<string>("topic");

                        try
                        {
                            switch (type?.ToLowerInvariant())
                            {
                                case "queue":
                                    if (!await mc.QueueExistsAsync(name))
                                    {
                                        await mc.CreateQueueAsync(name);
                                    }
                                    break;
                                case "topic":
                                    await CreateTopicIfNotExist(mc, name);
                                    break;
                                case "subscription":
                                    // if the topic doesn't exist, we'll create it 
                                    await CreateTopicIfNotExist(mc, name);
                                    if (!await mc.SubscriptionExistsAsync(topic, name))
                                    {
                                        await mc.CreateSubscriptionAsync(topic, name);
                                    }
                                    break;
                                case "link":
                                    // if the topic doesn't exist, we'll create it 
                                    await CreateTopicIfNotExist(mc, name);
                                    if (!await mc.SubscriptionExistsAsync(topic, name) &&
                                        await mc.QueueExistsAsync(name))
                                    {
                                        var sd = new SubscriptionDescription(topic, name)
                                        {
                                            ForwardTo = name
                                        };
                                        await mc.CreateSubscriptionAsync(sd);
                                    }
                                    break;
                                default:
                                    log.LogError($"Don't know how to create '{type}'");
                                    return;
                            }
                        }
                        catch(MessagingEntityAlreadyExistsException)
                        {
                            // means we got into a race condition where two commands
                            // tried to create the same entity concurrently. We'll
                            // swallow this exception.
                        }

                    }
                    break;
                case "delete":
                    {
                        var delete = JObject.Parse(Encoding.UTF8.GetString(message.Body));
                        var type = delete.Value<string>("type");
                        var name = delete.Value<string>("name");
                        var topic = delete.Value<string>("topic");
                        switch (type?.ToLowerInvariant())
                        {
                            case "queue":
                                if (await mc.QueueExistsAsync(name))
                                {
                                    await mc.DeleteQueueAsync(name);
                                }
                                break;
                            case "topic":
                                if (await mc.TopicExistsAsync(name))
                                {
                                    await mc.DeleteTopicAsync(name);
                                }
                                break;
                            case "subscription":
                                if (await mc.TopicExistsAsync(topic) && 
                                    await mc.SubscriptionExistsAsync(topic, name))
                                {
                                    await mc.DeleteSubscriptionAsync(topic, name);
                                }
                                break;
                            case "link":
                                if (await mc.TopicExistsAsync(topic) && 
                                    await mc.SubscriptionExistsAsync(topic, name))
                                {
                                    await mc.DeleteSubscriptionAsync(topic, name);
                                }
                                break;
                            default:
                                log.LogError($"Don't know how to delete '{type}'");
                                return;
                        }
                    }
                    break;
            }
        }

        private static async Task CreateTopicIfNotExist(ManagementClient mc, string name)
        {
            if (!await mc.TopicExistsAsync(name))
            {
                await mc.CreateTopicAsync(name);
            }
            if (!await mc.SubscriptionExistsAsync(name, "bridge"))
            {
                var sd = new SubscriptionDescription(name, "bridge")
                {
                    ForwardTo = "bridge"
                };
                var rl = new RuleDescription("bridge")
                {
                    Filter = new SqlFilter("xbridge is null"),
                    Action = new SqlRuleAction($"set xbridge=1; set sys.to='{name}'")
                };
                await mc.CreateSubscriptionAsync(sd, rl);
            }
        }
    }
}

