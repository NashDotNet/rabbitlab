using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace subscriber
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
                    channel.QueueDeclare( "sub-queue", false, false, true, null );
                    channel.QueueBind( "sub-queue", "pub-ex", "" );
                    channel.BasicConsume( "sub-queue", false, new SimpleConsumer( channel, OnMessage ) );
                    Console.ReadKey();
                }
            }
        }

        public static void OnMessage( byte[] message )
        {
            var text = Encoding.UTF8.GetString( message );
            Console.WriteLine( text );
        }
    }

    public class SimpleConsumer : IBasicConsumer
    {
        public void HandleBasicConsumeOk( string consumerTag )
        {
            
        }

        public void HandleBasicCancelOk( string consumerTag )
        {
            
        }

        public void HandleBasicCancel( string consumerTag )
        {
            
        }

        public void HandleModelShutdown( IModel model, ShutdownEventArgs reason )
        {
            
        }

        public void HandleBasicDeliver( 
            string consumerTag, 
            ulong deliveryTag, 
            bool redelivered, 
            string exchange, 
            string routingKey, 
            IBasicProperties properties, 
            byte[] body )
        {
            //Task.Factory.StartNew( () => 
            //    OnMessage( 
            //        body,
            //        () => Model.BasicAck( deliveryTag, false ),
            //        () => Model.BasicNack( deliveryTag, false, false )
            //        ) );
            OnMessage(
                body,
                () => Model.BasicAck( deliveryTag, false ),
                () => Model.BasicNack( deliveryTag, false, false )
                );
        }

        public IModel Model { get; set; }
        public Action<byte[], Action, Action> OnMessage { get; set; }

        public SimpleConsumer( IModel model, Action<byte[]> onMessage )
        {
            Model = model;
            OnMessage = ( message, ack, nack ) =>
                            {
                                try
                                {
                                    onMessage( message );
                                    ack();
                                }
                                catch ( Exception ex )
                                {
                                    nack();
                                }
                            };
        }
    }
}
