/*
 Linky Cmd - The command line program to be used with LinkyPIC

 The contents of this file are subject to the Mozilla Public License Version 1.1 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy of the
 License at http://www.mozilla.org/MPL/

 Software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTY OF
 ANY KIND, either express or implied. See the License for the specific language governing rights
 and limitations under the License.

 The Original Code is Program.cs.

 The Initial Developer of the Original Code is Olivier Sannier.
 Portions created by Olivier Sannier are Copyright (C) of Olivier Sannier. All rights reserved.
*/
using System;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Text;
using System.Text.Json;
using CommandLine;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Nest;
using System.Threading;
using log4net;
using log4net.Config;
using log4net.Core;
using log4net.Layout;
using log4net.Appender;
using log4net.Repository.Hierarchy;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;

namespace LinkyCmd
{
    class Program
    {
        private const int UDP_SERVER_PORT = 51;
        private const int TCP_SERVER_PORT = 561;
        private const int MAX_DISCOVERY_RETRY = 5;
        private const int MAX_RECONNECT_RETRY = 5;
        private const int MAX_CONSECUTIVE_INVALID_FRAMES = 10;

        private static readonly ILog log = LogManager.GetLogger(typeof(Program));

        static private IPAddress FindLinkyPIC()
        {
            IPEndPoint broadcastEndPoint = new IPEndPoint(IPAddress.Broadcast, UDP_SERVER_PORT);
            byte[] sendbuf = Encoding.ASCII.GetBytes("LinkyPIC");

            foreach(var adapter in NetworkInterface.GetAllNetworkInterfaces())
            {
                var properties = adapter.GetIPProperties();
                if (adapter.OperationalStatus == OperationalStatus.Up && adapter.Supports(NetworkInterfaceComponent.IPv4))
                {
                    foreach (var unicastAddress in properties.UnicastAddresses)
                    {
                        if (unicastAddress.Address.AddressFamily == AddressFamily.InterNetwork)
                        {
                            IPEndPoint adapterEndPoint = new IPEndPoint(unicastAddress.Address, 0);
                            UdpClient client = new UdpClient(adapterEndPoint); 
                            client.EnableBroadcast = true;

                            int retryCount = 0;
                            while (retryCount < MAX_DISCOVERY_RETRY)
                            {
                                client.Send(sendbuf, sendbuf.Length, broadcastEndPoint);

                                client.Client.ReceiveTimeout = 1500;
                                IPEndPoint? remoteEP = null;
                                try
                                {
                                    var data = client.Receive(ref remoteEP);
                                    if (data.Length == 4)
                                        return new IPAddress(data);
                                }
                                catch (SocketException e)
                                {
                                    if (e.SocketErrorCode != SocketError.TimedOut)
                                        throw;
                                }
                                retryCount++;
                            }
                            
                            client.Close();
                        }
                    }
                }
            }
            throw new Exception("Could not find LinkyPIC on the network");
        }

        class Options
        {
            [Option('l', "linkyPIC-address", Required = false, HelpText="IP address of the LinkyPIC system. If not provided, a UDP broadcast will be performed to find LinkyPIC")]
            public string? LinkyPICAddress { get; set; }
            
            [Option('e', "es-URL", Required = false, Default="http://localhost:9200", HelpText = "ElasticSearch URL to use when performing the post request. If indicated, MQTT publishing will not be performed")]
            public string? ElasticSearchURL { get; set; }

            [Option('i', "es-Index", Required = false, Default="linky", HelpText = "ElasticSearch index to post to")]
            public string? ElasticSearchIndex { get; set; }

            [Option('v', "verbose", Required = false, Default=false, HelpText = "Verbose output")]
            public bool Verbose { get; set; }

            [Option('s', "use-syslog", Required = false, Default = false, HelpText = "Send log to syslog")]
            public bool UseSyslog { get; set; }

            [Option("log4net-config-file", Required = false, HelpText = "The name of the log4net file to use. If indicated, completely overrides any other log related option")]
            public string? Log4NetConfigFile { get; set; }

            [Option('m', "mqtt-broker", Required = false, HelpText = "MQTT broker URL. If indicated, ElasticSearch posting will not be performed")]
            public string? MQTTBrokerURL { get; set; }

            [Option('t', "mqtt-topic", Required = false, Default = "linky", HelpText = "MQTT topic")]
            public string? MQTTTopic { get; set; }
        }

        static void recreateClient(IPAddress linkyPICAddress, ref TcpClient? client, out NetworkStream stream, out byte[] buffer)
        {
            if (client != null)
                client.Close();

            client = new TcpClient();
            client.Connect(linkyPICAddress.ToString(), TCP_SERVER_PORT);

            stream = client.GetStream();

            if (!stream.CanRead)
                throw new Exception("Network stream is not readable!");

            buffer = new byte[client.ReceiveBufferSize];
        }

        static void reconnect(IPAddress linkyPICAddress, ref TcpClient? client, ref NetworkStream stream, ref byte[] buffer)
        {
            int retryCount = 0;
            SocketException? lastReconnectException = null;
            while (retryCount < MAX_RECONNECT_RETRY)
            {
                lastReconnectException = null;
                log.Warn("Reconnecting...");
                try
                {
                    recreateClient(linkyPICAddress, ref client, out stream, out buffer);
                    log.Warn("Reconnection successful!");
                    break;
                }
                catch (SocketException e)
                {
                    log.Warn("/!\\  SocketException (" + e.Message +") while reconnecting, retrying...");
                    lastReconnectException = e;
                    retryCount++;
                }
            }

            if (lastReconnectException != null)
                throw new Exception("Exception while trying to reconnect", lastReconnectException);
        }

        static async Task<IManagedMqttClient> buildMqttClient(Options opts)
        {
            // Creates a new client
            MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                    .WithClientId("LinkyCMD")
                                                    .WithConnectionUri(opts.MQTTBrokerURL);

            // Create client options objects
            ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                    .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                    .WithClientOptions(builder.Build())
                                    .Build();

            // Creates the client object
            IManagedMqttClient mqttClient = new MqttFactory().CreateManagedMqttClient();

            mqttClient.ConnectedAsync += 
                (MqttClientConnectedEventArgs args) => 
                {
                    log.Debug("Connected to MQTT broker");
                    return Task.CompletedTask;
                };
            mqttClient.DisconnectedAsync +=
                (MqttClientDisconnectedEventArgs args) =>
                {
                    log.InfoFormat("Disconnected from MQTT broker: %s", args.ReasonString);
                    return Task.CompletedTask;
                };

            mqttClient.ConnectingFailedAsync += 
                (ConnectingFailedEventArgs args) =>
                {
                    log.WarnFormat("Connection to MQTT broker failed: %s", args.Exception.ToString());
                    return Task.CompletedTask;
                };

            // Starts a connection with the Broker
            await mqttClient.StartAsync(options); //.GetAwaiter().GetResult();

            return mqttClient;
        }

        static async Task RunWithValidOptions(Options opts)
        {
            IPAddress? linkyPICAddress = null;
            if (String.IsNullOrEmpty(opts.LinkyPICAddress))
                linkyPICAddress = FindLinkyPIC();
            else
                linkyPICAddress = IPAddress.Parse(opts.LinkyPICAddress);

            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());

            if (string.IsNullOrEmpty(opts.Log4NetConfigFile))
            {
                if (opts.Verbose)
                    logRepository.Threshold = Level.Debug;
                else
                    logRepository.Threshold = Level.Info;

                if (opts.UseSyslog)
                {
                    var layout = new PatternLayout();
                    layout.ConversionPattern = "%level - %message";
                    layout.ActivateOptions();

                    var appender = new LocalSyslogAppender();
                    appender.Facility = LocalSyslogAppender.SyslogFacility.Local0;
                    appender.Layout = layout;
                    appender.Identity = "LinkyCmd";
                    appender.ActivateOptions();

                    var hierarchy = (Hierarchy)logRepository;
                    hierarchy.Root.RemoveAllAppenders();
                    hierarchy.Root.AddAppender(appender);
                    hierarchy.Configured = true;
                }
            }
            else
            {
                ((Hierarchy)logRepository).Root.RemoveAllAppenders();
                XmlConfigurator.Configure(logRepository, new FileInfo(opts.Log4NetConfigFile));
            }

            log.Info("Talking to LinkyPIC on " + linkyPICAddress.ToString());

            bool useMQTT = !String.IsNullOrEmpty(opts.MQTTBrokerURL);
            
            TcpClient? client = null;
            try
            {
                using ConnectionSettings? esSettings = (useMQTT) ? null : new ConnectionSettings(new Uri(opts.ElasticSearchURL ?? "")).DefaultIndex(opts.ElasticSearchIndex);
                using IManagedMqttClient? mqttClient = (useMQTT) ? await buildMqttClient(opts) : null;

                recreateClient(linkyPICAddress, ref client, out var stream, out var buffer);
  
                MemoryStream frameStream = new MemoryStream();
                bool previousFrameWasEmpty = true;
                int consecutiveInvalidFramesCount = 0;
                while (true)
                {
                    do
                    {
                        int readCount = 0;
                        CancellationTokenSource cancellationSource = new CancellationTokenSource();
                        CancellationToken cancellationToken = cancellationSource.Token;

                        var task = stream.ReadAsync(buffer, 0, buffer.Length, cancellationToken);
                        if (task.Wait(5000) && task.Status == System.Threading.Tasks.TaskStatus.RanToCompletion) 
                        {
                            readCount = task.Result;
                        }
                        else
                        {
                            cancellationSource.Cancel();
                            log.Warn("/!\\ No answer in a timely manner");
                            reconnect(linkyPICAddress, ref client, ref stream, ref buffer);
                        }
                        if (readCount > 0)
                        {
                            int STXPos = -1;
                            int ETXPos = -1;

                            for(int bufferIndex = 0; bufferIndex < readCount; bufferIndex++)
                                switch (buffer[bufferIndex])
                                {
                                    case 0x02:
                                        STXPos = bufferIndex;
                                        break;
                                    case 0x03:
                                        ETXPos = bufferIndex;
                                        break;
                                }

                            if (ETXPos >= 0)
                            {
                                if (frameStream.Length > 0)
                                {
                                    if (ETXPos > 0)
                                        frameStream.Write(buffer, 0, ETXPos - 1);

                                    frameStream.Position = 0;
                                    Frame newFrame = new Frame(frameStream);

                                    if (newFrame.IsEmpty)
                                    {
                                        if (!previousFrameWasEmpty)
                                        {
                                            previousFrameWasEmpty = true;
                                            log.Warn("/!\\ Empty frame received, check Linky connectivity");
                                        }
                                    }
                                    else 
                                    {
                                        previousFrameWasEmpty = false;
                                        log.Debug(newFrame.ToString());

                                        // only send valid frames
                                        if (newFrame.IsValid)
                                        {
                                            consecutiveInvalidFramesCount = 0;

                                            if (useMQTT)
                                            {
                                                var JsonFrame = JsonSerializer.Serialize(newFrame);
                                                await mqttClient!.EnqueueAsync(opts.MQTTTopic, JsonFrame);
                                            }
                                            else
                                            {
                                                // https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
                                                var esClient = new ElasticClient(esSettings);
                                                var indexResponse = esClient.IndexDocument(newFrame);
                                                if (indexResponse.Result != Result.Created) 
                                                {
                                                    log.Error(indexResponse);
                                                    log.Error(indexResponse.ServerError);
                                                }
                                            }
                                        }
                                        else 
                                        {
                                            consecutiveInvalidFramesCount++;
                                            if (consecutiveInvalidFramesCount > MAX_CONSECUTIVE_INVALID_FRAMES)
                                            {
                                                log.Warn("/!\\ Too many consecutive invalid frames");
                                                reconnect(linkyPICAddress, ref client, ref stream, ref buffer);
                                                consecutiveInvalidFramesCount = 0;
                                            }
                                        }
                                    }
                                }
                            }

                            if (STXPos >= 0)
                            {
                                frameStream.SetLength(0);
                                frameStream.Write(buffer, STXPos + 1, readCount - STXPos);
                            }
                            else if (frameStream.Length > 0)
                            {
                                frameStream.Write(buffer, 0, readCount);
                            }

                            //log.Debug(Encoding.ASCII.GetString(buffer, 0, readCount));
                        }
                    }
                    while (stream.DataAvailable);
                }
            }
            finally 
            {
                client?.Close();
            }
        }

        static async Task Main(string[] args)
        {
            try
            {
                var hierarchy = (Hierarchy)LogManager.GetRepository(Assembly.GetEntryAssembly());
                var layout = new PatternLayout();
                layout.ConversionPattern = "%date %level - %message%newline";
                layout.ActivateOptions();

                var appender = new ManagedColoredConsoleAppender();
                appender.Layout = layout;
                appender.ActivateOptions();

                hierarchy.Root.AddAppender(appender);
                hierarchy.Configured = true;

                try
                {
                    var parserResult = CommandLine.Parser.Default.ParseArguments<Options>(args);
                    if (!parserResult.Errors.Any())
                        await RunWithValidOptions(parserResult.Value);
                }
                catch (Exception e)
                {
                    log.Fatal("Exception occurred:" + Environment.NewLine + e);
                }
                }
            catch (Exception e)
            {
               System.Console.WriteLine("Exception occurred before logger setup:" + Environment.NewLine + e);
            }
        }
    }
}
