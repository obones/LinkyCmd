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

                            int retryCount = 0;
                            while (retryCount < MAX_DISCOVERY_RETRY)
                            {
                                client.Send(sendbuf, sendbuf.Length, broadcastEndPoint);

                                client.Client.ReceiveTimeout = 1500;
                                IPEndPoint remoteEP = null;
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
            [Option('l', "linkyPIC-address", Required = false, HelpText="IP address of the LinkyPIC system")]
            public string LinkyPICAddress { get; set; }
            
            [Option('e', "es-URL", Required = false, Default="http://localhost:9200", HelpText = "ElasticSearch URL to use when performing the post request")]
            public string ElasticSearchURL { get; set; }

            [Option('i', "es-Index", Required = false, Default="linky", HelpText = "ElasticSearch index to post to")]
            public string ElasticSearchIndex { get; set; }

            [Option('v', "verbose", Required = false, Default=false, HelpText = "Verbose output")]
            public bool Verbose { get; set; }

            [Option('s', "use-syslog", Required = false, Default = false, HelpText = "Send log to syslog")]
            public bool UseSyslog { get; set; }
        }

        static void recreateClient(IPAddress linkyPICAddress, ref TcpClient client, ref NetworkStream stream, ref byte[] buffer)
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

        static void reconnect(IPAddress linkyPICAddress, ref TcpClient client, ref NetworkStream stream, ref byte[] buffer)
        {
            int retryCount = 0;
            SocketException lastReconnectException = null;
            while (retryCount < MAX_RECONNECT_RETRY)
            {
                lastReconnectException = null;
                log.Warn("Reconnecting...");
                try
                {
                    recreateClient(linkyPICAddress, ref client, ref stream, ref buffer);
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

        static void RunWithValidOptions(Options opts)
        {
            IPAddress linkyPICAddress = null;
            if (String.IsNullOrEmpty(opts.LinkyPICAddress))
                linkyPICAddress = FindLinkyPIC();
            else
                linkyPICAddress = IPAddress.Parse(opts.LinkyPICAddress);

            var logRepository = LogManager.GetRepository(Assembly.GetEntryAssembly());
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

                var hiearchy = (Hierarchy)logRepository;
                hiearchy.Root.AddAppender(appender);
                hiearchy.Configured = true;
            }

            log.Info("Talking to LinkyPIC on " + linkyPICAddress.ToString());
            
            TcpClient client = null;
            NetworkStream stream = null;
            byte[] buffer = null;
            try
            {
                recreateClient(linkyPICAddress, ref client, ref stream, ref buffer);

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

                                            // https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
                                            var esSettings = new ConnectionSettings(new Uri(opts.ElasticSearchURL)).DefaultIndex(opts.ElasticSearchIndex);
                                            var esClient = new ElasticClient(esSettings);
                                            var indexResponse = esClient.IndexDocument(newFrame);
                                            if (indexResponse.Result != Result.Created) 
                                            {
                                                log.Error(indexResponse);
                                                log.Error(indexResponse.ServerError);
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
                if (client != null)
                    client.Close();
            }
        }

        static void HandleParseErrors(IEnumerable<Error> errors)
        {
            foreach(var error in errors)
            {
                // no need to display, it's already done by the CommandLine library
                //log.Error("Parse error: " + error);
            }
        }

        static void Main(string[] args)
        {
            try
            {
                var hiearchy = (Hierarchy)LogManager.GetRepository(Assembly.GetEntryAssembly());
                var layout = new PatternLayout();
                layout.ConversionPattern = "%date %level - %message%newline";
                layout.ActivateOptions();

                var appender = new ManagedColoredConsoleAppender();
                appender.Layout = layout;
                appender.ActivateOptions();

                hiearchy.Root.AddAppender(appender);
                hiearchy.Configured = true;

                try
                {
                    //XmlConfigurator.Configure(logRepository, new FileInfo("log4net.config"));

                    CommandLine.Parser.Default.ParseArguments<Options>(args)
                        .WithParsed<Options>((opts) => RunWithValidOptions(opts))
                        .WithNotParsed<Options>((errs) => HandleParseErrors(errs));
                }
                catch (Exception e)
                {
                    log.Fatal("Exception occured:" + Environment.NewLine + e);
                }
                }
            catch (Exception e)
            {
               System.Console.WriteLine("Exception occured before logger setup:" + Environment.NewLine + e);
            }
        }
    }
}
