using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using RabbitMQ.Client;

namespace publisher
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionFactory = new ConnectionFactory
                                        {
                                            HostName = "localhost",
                                            Port = 5672,
                                            UserName = "guest",
                                            Password = "guest",
                                            VirtualHost = "/",
                                            Protocol = Protocols.AMQP_0_9_1,
                                            RequestedFrameMax = uint.MaxValue,
                                            RequestedHeartbeat = ushort.MaxValue
                                        };

            using( var connection = connectionFactory.CreateConnection() )
            {
                using( var channel = connection.CreateModel() )
                {
                    channel.ExchangeDeclare( "pub-ex", ExchangeType.Fanout, false, true, null );
                     
                    var properties = channel.CreateBasicProperties();
                    Console.WriteLine("Press any key to begin publishing messages.");
                    Console.ReadKey();
                    
                    using( var reader = new StreamReader( @"..\pub-source.txt", Encoding.UTF8 ) )
                    {
                        var line = "";
                        while( !string.IsNullOrEmpty( ( line = reader.ReadLine() ) ) )
                        {
                            Console.WriteLine( line );
                            var bytes = Encoding.UTF8.GetBytes( line );
                            channel.BasicPublish( "pub-ex", "", false, false, properties, bytes );
                        }
                    }
                }
            }
        }
    }
}
