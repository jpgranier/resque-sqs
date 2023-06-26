class MockSQSClient
  def initialize(queue_urls = [])
    @queues = {}
    queue_urls.each do |queue_url|
      raise "duplicate queue of #{queue_url} provided" if @queues.key?(queue_url)

      @queues[queue_url.to_s] = NonThreadedQueue.new
    end
  end

  def config
    Config.new
  end

  def send_message(queue_url:, message_body:)
    queue = get_queue(queue_url)
    receive_message_result = MockReceiveMessageResult.new
    receive_message_result.add_message(message_body)
    queue.push(receive_message_result)
    MockSendMessageResult.new
  end

  def receive_message(queue_url:, max_number_of_messages:, wait_time_seconds:)
    queue = get_queue(queue_url)
    messages = []
    max_number_of_messages.times do |_i|
      obj = queue.pop
      break if obj.nil?

      messages << obj.messages.first
    end
    MockReceiveMessageResult.new(messages)
  end

  # This is just mocking the deletion of a hidden SQS object. No need to do anything.
  def delete_message(queue_url:, receipt_handle:)
    MockSQSClient::MockDeleteMessageResult.new
  end

  def get_queue_attributes(queue_url:, attribute_names:)
    queue = get_queue(queue_url)

    MockSQSClient::MockGetQueueAttributeResult.new(queue.length, attribute_names)
  end

  def purge_queue(queue_url:)
    queue = get_queue(queue_url)
    queue.clear

    MockSQSClient::MockPurgeQueueResult.new
  end

  def list_queues(queue_name_prefix:)
    MockSQSClient::MockListQueuesResult.new(@queues.keys)
  end

  def add_queue(queue_name)
    @queues[queue_name.to_s] = NonThreadedQueue.new
  end

  def remove_queue(queue_name)
    get_queue(queue_name)

    @queues.delete(queue_name.to_s)
  end

  def purge_all_queues
    @queues = {}
  end

  private

  def get_queue(queue_url)
    raise 'queue_url not provided' if queue_url.nil? || queue_url.empty?

    raise "queue_url #{queue_url} does not exist" unless @queues.key?(queue_url.to_s)

    @queues[queue_url.to_s]
  end

  class MockSendMessageResult
    def successful?
      true
    end
  end

  class MockReceiveMessageResult
    attr_reader :messages

    def initialize(messages = [])
      @messages = messages
    end

    def add_message(message_body)
      @messages << MockReceiveMessage.new(SecureRandom.hex, message_body)
    end

    def successful?
      true
    end

    class MockReceiveMessage
      attr_reader :receipt_handle, :body

      def initialize(receipt_handle, message_body)
        @receipt_handle = receipt_handle
        @body = message_body
      end
    end
  end

  class MockDeleteMessageResult
    def successful?
      true
    end
  end

  class MockGetQueueAttributeResult

    attr_reader :attributes

    def initialize(length, attribute_names)
      if attribute_names != %w[ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible]
        raise 'only length calculations are supported right now'
      end

      raise 'a length of less than zero should not be passed in' if length < 0

      @attributes = {}
      @attributes['ApproximateNumberOfMessages'] = length / 2
      @attributes['ApproximateNumberOfMessagesNotVisible'] = length / 2
      @attributes['ApproximateNumberOfMessages'] += 1 if length % 2 != 0
    end

    def successful?
      true
    end
  end

  class MockPurgeQueueResult
    def successful?
      true
    end
  end

  class MockListQueuesResult
    attr_reader :queue_urls, :next_token

    def initialize(queue_urls)
      @queue_urls = queue_urls
      @next_token = nil
    end

    def successful?
      true
    end
  end

  class NonThreadedQueue
    def initialize
      @queue = []
    end

    def push(obj)
      @queue << obj
    end

    def pop
      @queue.shift
    end

    def length
      @queue.length
    end

    def clear
      @queue = []
    end
  end

  class Config
    def region
      'us-east-1'
    end
  end
end
