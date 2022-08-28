<?php
    
    declare(strict_types=1);
    
    namespace Rapide\LaravelQueueKafka\Queue\Jobs;
    
    use Illuminate\Container\Container;
    use Illuminate\Contracts\Queue\Job as JobContract;
    use Illuminate\Queue\Jobs\Job;
    use Illuminate\Queue\Jobs\JobName;
    use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
    use Rapide\LaravelQueueKafka\Queue\KafkaQueue;
    use RdKafka\ConsumerTopic;
    use RdKafka\Message;
    
    class KafkaJob extends Job implements JobContract
    {
        protected KafkaQueue $connection;
        
        protected $queue;
        
        protected Message $message;
        
        protected ConsumerTopic $topic;
        
        public function __construct(
            Container $container,
            KafkaQueue $connection,
            Message $message,
            $connectionName,
            $queue,
            ConsumerTopic $topic
        ) {
            $this->container = $container;
            $this->connection = $connection;
            $this->message = $message;
            $this->connectionName = $connectionName;
            $this->queue = $queue;
            $this->topic = $topic;
        }
        
        public function fire(): void
        {
            $payload = $this->payload();
            
            [$class, $method] = JobName::parse($payload['job']);
            
            with($this->instance = $this->resolve($class))->{$method}($this, $payload['data']);
        }
        
        public function attempts(): int
        {
            return (int)($this->payload()['attempts']) + 1;
        }
        
        public function getRawBody(): string
        {
            return $this->message->payload;
        }
        
        public function delete(): void
        {
            try {
                parent::delete();
                $this->topic->offsetStore($this->message->partition, $this->message->offset);
            } catch (\RdKafka\Exception $exception) {
                throw new QueueKafkaException('Could not delete job from the queue', 0, $exception);
            }
        }
        
        public function release($delay = 0): void
        {
            parent::release($delay);
            
            $this->delete();
            
            $body = $this->payload();
            
            if (isset($body['data']['command']) === true) {
                $job = $this->unserialize($body);
            } else {
                $job = $this->getName();
            }
            
            $data = $body['data'];
            
            $this->connection->push($job, $data, $this->getQueue());
        }
        
        public function getJobId(): string
        {
            return $this->message->key;
        }
        
        private function unserialize(array $body): string
        {
            return unserialize($body['data']['command'], ['allowed_classes' => true]);
        }
    }
