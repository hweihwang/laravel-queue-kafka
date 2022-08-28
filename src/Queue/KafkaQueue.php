<?php

    declare(strict_types=1);

    namespace Rapide\LaravelQueueKafka\Queue;

    use ErrorException;
    use Exception;
    use Illuminate\Contracts\Queue\Queue as QueueContract;
    use Illuminate\Queue\Queue;
    use Rapide\LaravelQueueKafka\Exceptions\QueueKafkaException;
    use Rapide\LaravelQueueKafka\Queue\Jobs\KafkaJob;
    use RdKafka\Consumer;
    use RdKafka\Producer;
    use RdKafka\ProducerTopic;
    use RdKafka\TopicConf;

    class KafkaQueue extends Queue implements QueueContract
    {

        protected string $defaultQueue;

        protected int $sleepOnError;

        protected array $config;

        private string $correlationId;

        private Producer $producer;

        private Consumer $consumer;

        private array $topics = [];

        private array $queues = [];

        public function __construct(Producer $producer, Consumer $consumer, $config)
        {
            $this->defaultQueue = $config['queue'];
            $this->sleepOnError = $config['sleep_on_error'] ?? 5;

            $this->producer = $producer;
            $this->consumer = $consumer;
            $this->config = $config;
        }

        public function size($queue = null): int
        {
            return 1;
        }

        public function push($job, $data = '', $queue = null): string
        {
            return $this->pushRaw($this->createPayload($job, $queue, $data), $queue, []);
        }

        public function pushRaw($payload, $queue = null, array $options = []): string
        {
            try {
                $topic = $this->getTopic($queue);

                $pushRawCorrelationId = $this->getCorrelationId();

                $topic->produce(RD_KAFKA_PARTITION_UA, 0, $payload, $pushRawCorrelationId);

                return $pushRawCorrelationId;
            } catch (ErrorException $exception) {
                $this->reportConnectionError('pushRaw', $exception);
            }
        }

        public function later($delay, $job, $data = '', $queue = null): void
        {
            throw new QueueKafkaException('Later not yet implemented');
        }

        public function pop($queue = null)
        {
            try {
                $queue = $this->getQueueName($queue);
                if (!array_key_exists($queue, $this->queues)) {
                    $this->queues[$queue] = $this->consumer->newQueue();
                    $topicConf = new TopicConf();
                    $topicConf->set('auto.offset.reset', 'largest');

                    $this->topics[$queue] = $this->consumer->newTopic($queue, $topicConf);
                    $this->topics[$queue]->consumeQueueStart(0, RD_KAFKA_OFFSET_STORED, $this->queues[$queue]);
                }

                $message = $this->queues[$queue]->consume(1000);

                if ($message === null) {
                    return null;
                }

                switch ($message->err) {
                    case RD_KAFKA_RESP_ERR_NO_ERROR:
                        return new KafkaJob(
                            $this->container, $this, $message,
                            $this->connectionName, $queue ?: $this->defaultQueue, $this->topics[$queue]
                        );
                    case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    case RD_KAFKA_RESP_ERR__TIMED_OUT:
                        break;
                    default:
                        throw new QueueKafkaException($message->errstr(), $message->err);
                }
            } catch (\RdKafka\Exception $exception) {
                throw new QueueKafkaException('Could not pop from the queue', 0, $exception);
            }
        }

        private function getQueueName($queue): string
        {
            return $queue ?: $this->defaultQueue;
        }

        private function getTopic($queue): ProducerTopic
        {
            return $this->producer->newTopic($this->getQueueName($queue));
        }

        public function setCorrelationId($id): void
        {
            $this->correlationId = $id;
        }

        public function getCorrelationId(): string
        {
            return $this->correlationId ?: uniqid('', true);
        }

        public function getConfig(): array
        {
            return $this->config;
        }

        protected function createPayloadArray($job, $queue = null, $data = ''): array
        {
            return array_merge(parent::createPayloadArray($job, $queue, $data), [
                'id' => $this->getCorrelationId(),
                'attempts' => 0,
            ]);
        }

        protected function reportConnectionError($action, Exception $e): void
        {
            if ($this->sleepOnError === false) {
                throw new QueueKafkaException('Error writing data to the connection with Kafka');
            }

            sleep($this->sleepOnError);
        }
    }
