using System;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Text;
using CommandLine;
using System.Collections.Generic;
using System.IO;
using Nest;
using System.Threading;

namespace LinkyCmd
{
    class Program
    {
        private const int UDP_SERVER_PORT = 51;
        private const int TCP_SERVER_PORT = 561;
        private const int MAX_DISCOVERY_RETRY = 5;

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
        }

        static void recreateClient(IPAddress linkyPICAddress, ref TcpClient client, ref NetworkStream stream, ref byte[] buffer, out DateTime lastCreationTimeStamp)
        {
            if (client != null)
                client.Close();

            client = new TcpClient();
            client.Connect(linkyPICAddress.ToString(), TCP_SERVER_PORT);

            stream = client.GetStream();

            if (!stream.CanRead)
                throw new Exception("Network stream is not readable!");

            buffer = new byte[client.ReceiveBufferSize];

            lastCreationTimeStamp = DateTime.Now;
        }

        static void RunWithValidOptions(Options opts)
        {
            IPAddress linkyPICAddress = null;
            if (String.IsNullOrEmpty(opts.LinkyPICAddress))
                linkyPICAddress = FindLinkyPIC();
            else
                linkyPICAddress = IPAddress.Parse(opts.LinkyPICAddress);

            System.Console.WriteLine("Talking to LinkyPIC on " + linkyPICAddress.ToString());
            
            TcpClient client = null;
            NetworkStream stream = null;
            byte[] buffer = null;
            DateTime lastClientCreationTimeStamp = DateTime.MinValue;
            try
            {
                recreateClient(linkyPICAddress, ref client, ref stream, ref buffer, out lastClientCreationTimeStamp);

                MemoryStream frameStream = new MemoryStream();
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
                            System.Console.WriteLine("No answer in a timely manner, reconnecting");
                            recreateClient(linkyPICAddress, ref client, ref stream, ref buffer, out lastClientCreationTimeStamp);
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
                                    System.Console.WriteLine(newFrame.ToString());

                                    // only send valid frames
                                    if (newFrame.IsValid)
                                    {
                                        // https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
                                        var esSettings = new ConnectionSettings(new Uri(opts.ElasticSearchURL)).DefaultIndex(opts.ElasticSearchIndex);
                                        var esClient = new ElasticClient(esSettings);
                                        var indexResponse = esClient.IndexDocument(newFrame);
                                        if (indexResponse.Result != Result.Created) 
                                        {
                                            System.Console.WriteLine(indexResponse);
                                            System.Console.WriteLine(indexResponse.ServerError);
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

                            //System.Console.Write(Encoding.ASCII.GetString(buffer, 0, readCount));
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
                //System.Console.WriteLine("Parse error: " + error);
            }
        }

        static void Main(string[] args)
        {
            try
            {
                CommandLine.Parser.Default.ParseArguments<Options>(args)
                    .WithParsed<Options>((opts) => RunWithValidOptions(opts))
                    .WithNotParsed<Options>((errs) => HandleParseErrors(errs));
            }
            catch (Exception e)
            {
                System.Console.Write("Exception occured:" + Environment.NewLine + e);
            }
        }
    }
}
