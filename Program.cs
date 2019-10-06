using System;
using System.Net;
using System.Net.Sockets;
using System.Net.NetworkInformation;
using System.Text;
using CommandLine;
using System.Collections.Generic;

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

                                var task = client.ReceiveAsync();
                                if (task.Wait(1000))// && !task.IsCanceled)
                                {
                                    var data = task.Result.Buffer;
                                    if (data.Length == 4)
                                        return new IPAddress(data);
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
            
            [Option('e', "es-URL", Required = false, Default="http://localhost/linky", HelpText = "ElasticSearch URL to use when performing the post request")]
            public string ElasticSearchURL { get; set; }
        }

        static void RunWithValidOptions(Options opts)
        {
            IPAddress linkyPICAddress = null;
            if (String.IsNullOrEmpty(opts.LinkyPICAddress))
                linkyPICAddress = FindLinkyPIC();
            else
                linkyPICAddress = IPAddress.Parse(opts.LinkyPICAddress);

            System.Console.WriteLine("Talking to LinkyPIC on " + linkyPICAddress.ToString());
            TcpClient client = new TcpClient();
            client.Connect(linkyPICAddress.ToString(), TCP_SERVER_PORT);

            NetworkStream stream = client.GetStream();

            if (!stream.CanRead)
                throw new Exception("Network stream is not readable!");

            byte[] buffer = new byte[client.ReceiveBufferSize];

            int readCount = stream.Read(buffer, 0, buffer.Length);
            if (readCount > 0)
            {
                System.Console.Write(Encoding.ASCII.GetString(buffer, 0, readCount));

                // https://www.elastic.co/guide/en/elasticsearch/client/net-api/current/nest-getting-started.html
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
