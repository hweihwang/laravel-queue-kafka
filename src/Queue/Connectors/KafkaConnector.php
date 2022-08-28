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
            $topicConf = $this->container->makeWith('queue.kafka.topic_conf', []);
            $topicConf->set('auto.offset.reset', 'largest');
            
            $producerConf = $this->container->makeWith('queue.kafka.conf', []);
            $producerConf->set('bootstrap.servers', $config['brokers']);
           
            $producer = $this->container->makeWith('queue.kafka.producer', ['conf' => $producerConf]);
            $producer->addBrokers($config['brokers']);

            $consumerConf = $this->container->makeWith('queue.kafka.conf', []);
            $consumerConf->set('group.id', $config['consumer_group_id'] ?? 'php-pubsub');
            $consumerConf->set('metadata.broker.list', $config['brokers']);
            $consumerConf->set('enable.auto.commit', 'true');
            $consumerConf->setDefaultTopicConf($topicConf);

            $consumer = $this->container->makeWith('queue.kafka.consumer', ['conf' => $consumerConf]);

            return new KafkaQueue(
                $producer,
                $consumer,
                $config
            );
        }
    }
