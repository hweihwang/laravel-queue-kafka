<?php
    
    declare(strict_types=1);
    
    namespace Rapide\LaravelQueueKafka\Queue\Connectors;
    
    use Illuminate\Container\Container;
    use Illuminate\Contracts\Container\BindingResolutionException;
    use Illuminate\Queue\Connectors\ConnectorInterface;
    use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
    use RdKafka\Conf;
    use RdKafka\Consumer;
    use RdKafka\Producer;
    use RdKafka\TopicConf;
    
    class KafkaConnector implements ConnectorInterface
    {
        private Container $container;
        
        public function __construct(Container $container)
        {
            $this->container = $container;
        }
        
        /**
         * @throws BindingResolutionException
         */
        public function connect(array $config): KafkaQueue
        {
            $producer = $this->container->makeWith('queue.kafka.producer', []);
            $producer->addBrokers($config['brokers']);
            
            $topicConf = $this->container->makeWith('queue.kafka.topic_conf', []);
            $topicConf->set('auto.offset.reset', 'largest');
            
            $conf = $this->container->makeWith('queue.kafka.conf', []);
            if (true === $config['sasl_enable']) {
                $conf->set('sasl.mechanisms', 'PLAIN');
                $conf->set('sasl.username', $config['sasl_plain_username']);
                $conf->set('sasl.password', $config['sasl_plain_password']);
                $conf->set('ssl.ca.location', $config['ssl_ca_location']);
            }
            $conf->set('group.id', $config['consumer_group_id'] ?? 'php-pubsub');
            $conf->set('metadata.broker.list', $config['brokers']);
            $conf->set('enable.auto.commit', 'false');
            $conf->set('offset.store.method', 'broker');
            $conf->setDefaultTopicConf($topicConf);
            
            $consumer = $this->container->makeWith('queue.kafka.consumer', ['conf' => $conf]);
            
            return new KafkaQueue(
                $producer,
                $consumer,
                $config
            );
        }
    }
