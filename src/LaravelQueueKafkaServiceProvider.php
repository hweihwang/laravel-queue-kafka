<?php

namespace Rapide\LaravelQueueKafka;

use Illuminate\Support\ServiceProvider;
use Rapide\LaravelQueueKafka\Queue\Connectors\KafkaConnector;

class LaravelQueueKafkaServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        $this->mergeConfigFrom(
            __DIR__ . '/../config/kafka.php', 'queue.connections.kafka'
        );

        $this->registerDependencies();
    }
    
    public function boot(): void
    {
        $queue = $this->app['queue'];
        $connector = new KafkaConnector($this->app);

        $queue->addConnector('kafka', function () use ($connector) {
            return $connector;
        });
    }
    
    protected function registerDependencies(): void
    {
        $this->app->bind('queue.kafka.topic_conf', function () {
            return new \RdKafka\TopicConf();
        });

        $this->app->bind('queue.kafka.producer', function ($app, $parameters) {
            return new \RdKafka\Producer($parameters['conf']);
        });

        $this->app->bind('queue.kafka.conf', function () {
            return new \RdKafka\Conf();
        });

        $this->app->bind('queue.kafka.consumer', function ($app, $parameters) {
            return new \RdKafka\Consumer($parameters['conf']);
        });
    }
    
    public function provides(): array
    {
        return [
            'queue.kafka.topic_conf',
            'queue.kafka.producer',
            'queue.kafka.consumer',
            'queue.kafka.conf',
        ];
    }
}
