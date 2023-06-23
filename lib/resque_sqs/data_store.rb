module ResqueSqs
  # An interface between Resque's persistence and the actual
  # implementation.
  class DataStore
    extend Forwardable

    HEARTBEAT_KEY = "workers:heartbeat"

    attr_reader :redis
    attr_reader :sqs

    def initialize(redis_connection, sqs_client)
      self.redis = redis_connection
      self.sqs = sqs_client
    end

    def_delegators :@queue_access, :push_to_queue,
                                   :pop_from_queue,
                                   :queue_size,
                                   :queue_names,
                                   :purge_queue,
                                   :remove_from_queue,
                                   :watch_queue,
                                   :queue_exists?

    def_delegators :@failed_queue_access, :add_failed_queue,
                                          :remove_failed_queue,
                                          :num_failed,
                                          :failed_queue_names,
                                          :push_to_failed_queue,
                                          :clear_failed_queue,
                                          :update_item_in_failed_queue,
                                          :remove_from_failed_queue,
                                          :list_failed_queue_range

    def_delegators :@workers, :worker_ids,
                              :workers_map,
                              :get_worker_payload,
                              :worker_exists?,
                              :register_worker,
                              :worker_started,
                              :unregister_worker,
                              :heartbeat,
                              :heartbeat!,
                              :remove_heartbeat,
                              :all_heartbeats,
                              :acquire_pruning_dead_worker_lock,
                              :set_worker_payload,
                              :worker_start_time,
                              :worker_done_working

    def_delegators :@stats_access, :clear_stat,
                                   :decrement_stat,
                                   :increment_stat,
                                   :stat

    def decremet_stat(*args)
      warn '[Resque] [Deprecation] ResqueSqs::DataStore #decremet_stat method is deprecated (please use #decrement_stat)'
      decrement_stat(*args)
    end

    # Compatibility with any non-Resque classes that were using ResqueSqs.redis as a way to access Redis
    def method_missing(sym,*args,&block)
      # TODO: deprecation warning?
      @redis.send(sym,*args,&block)
    end

    # make use respond like redis
    def respond_to?(method,include_all=false)
      @redis.respond_to?(method,include_all) || super
    end

    # Get a string identifying the underlying server.
    # Probably should be private, but was public so must stay public
    def identifier
      @redis.inspect
    end

    # Force a reconnect to Redis without closing the connection in the parent
    # process after a fork.
    def reconnect
      # Reconnect to Redis to avoid sharing a connection with the parent,
      # retry up to 3 times with increasing delay before giving up.
      tries = 0
      begin
        @redis.client.reconnect
      rescue Redis::BaseConnectionError
        if (tries += 1) <= 3
          log "Error reconnecting to Redis; retrying"
          sleep(tries)
          retry
        else
          log "Error reconnecting to Redis; quitting"
          raise
        end
      end
    end

    # Returns an array of all known Resque keys in Redis. Redis' KEYS operation
    # is O(N) for the keyspace, so be careful - this can be slow for big databases.
    def all_resque_keys
      @redis.keys("*").map do |key|
        key.sub("#{@redis.namespace}:", '')
      end
    end

    def server_time
      time, _ = @redis.time
      Time.at(time)
    end

    private

    def redis=(redis_connection)
      case redis_connection
      when String
        if redis_connection =~ /rediss?\:\/\//
          redis = Redis.new(:url => client)
        else
          redis_connection, namespace = redis_connection.split('/', 2)
          host, port, db = redis_connection.split(':')
          redis = Redis.new(:host => host, :port => port, :db => db)
        end
        namespace ||= :resque
        @redis = Redis::Namespace.new(namespace, :redis => redis)
      when Redis::Namespace
        @redis = redis_connection
      when Hash
        @redis = Redis::Namespace.new(:resque, :redis => Redis.new(redis_connection))
      else
        @redis = Redis::Namespace.new(:resque, :redis => redis_connection)
      end

      @failed_queue_access = FailedQueueAccess.new(@redis)
      @workers = Workers.new(@redis)
      @stats_access = StatsAccess.new(@redis)
    end

    def sqs=(sqs)
      @sqs = sqs
      @queue_access = QueueAccess.new(@sqs)
    end

    class QueueAccess
      MAX_NUMBER_OF_MESSAGES = 1

      def initialize(sqs)
        @sqs = sqs
      end

      def push_to_queue(queue, encoded_item)
        # TODO: Is there anything else that will validate the message was delivered to SQS?
        send_message_result = @sqs.send_message({
                                                  queue_url: queue,
                                                  message_body: encoded_item
                                                })
        raise "failed to deliver to #{queue}" unless send_message_result.successful?

        true
      end

      # Pop whatever is on queue
      def pop_from_queue(queue)
        receive_message_result = @sqs.receive_message(
          queue_url: queue,
          max_number_of_messages: MAX_NUMBER_OF_MESSAGES
        )
        raise "failed to pop_from_queue #{queue}" unless receive_message_result.successful?

        if receive_message_result.messages.length > MAX_NUMBER_OF_MESSAGES
          raise 'The number of requested messages did not match the number returned messages'
        end

        message = receive_message_result.messages.first
        return [nil, nil] if message.nil?

        raise 'receipt_handle is blank' if message.receipt_handle.to_s.empty?

        [message.receipt_handle, message.body]
      end

      # Get the number of items in the queue
      def queue_size(queue)
        get_queue_attributes_result = @sqs.get_queue_attributes(
          queue_url: queue,
          attribute_names: %w[ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible]
        )
        raise 'unable to get queue_size' unless get_queue_attributes_result.successful?

        get_queue_attributes_result.attributes['ApproximateNumberOfMessages'].to_i + get_queue_attributes_result.attributes['ApproximateNumberOfMessagesNotVisible'].to_i
      end

      def queue_names
        fetch_queues
      end

      def purge_queue(queue)
        purge_queue_result = @sqs.purge_queue(queue_url: queue)
        raise 'failed to purge_queue' unless purge_queue_result.successful?
      end

      def remove_from_queue(queue, receipt_handle)
        delete_message_result = @sqs.delete_message(
          queue_url: queue,
          receipt_handle: receipt_handle
        )
        unless delete_message_result.successful?
          raise "failed to delete message from queue #{queue} with receipt_handle #{receipt_handle}"
        end
      end

      # Private: do not call
      def watch_queue(queue, redis: @redis)
        redis.sadd(:queues, [queue.to_s])
      end

      def queue_exists?(queue)
        queue_names.member?(queue)
      end

      private

      def fetch_queues
        queues = []
        loop do
          list_queues_result = @sqs.list_queues
          raise 'unable to fetch queue_names' unless list_queues_result.successful?

          queues.concat(list_queues_result.queue_urls.to_a)
          break if list_queues_result.next_token.nil?
        end
        queues
      end
    end

    class FailedQueueAccess
      def initialize(redis)
        @redis = redis
      end

      def add_failed_queue(failed_queue_name)
        @redis.sadd(:failed_queues, [failed_queue_name])
      end

      def remove_failed_queue(failed_queue_name=:failed)
        @redis.del(failed_queue_name)
      end

      def num_failed(failed_queue_name=:failed)
        @redis.llen(failed_queue_name).to_i
      end

      def failed_queue_names(find_queue_names_in_key=nil)
        if find_queue_names_in_key.nil?
          [:failed]
        else
          Array(@redis.smembers(find_queue_names_in_key))
        end
      end

      def push_to_failed_queue(data,failed_queue_name=:failed)
        @redis.rpush(failed_queue_name,data)
      end

      def clear_failed_queue(failed_queue_name=:failed)
        @redis.del(failed_queue_name)
      end

      def update_item_in_failed_queue(index_in_failed_queue,new_item_data,failed_queue_name=:failed)
        @redis.lset(failed_queue_name, index_in_failed_queue, new_item_data)
      end

      def remove_from_failed_queue(index_in_failed_queue,failed_queue_name=nil)
        failed_queue_name ||= :failed
        hopefully_unique_value_we_can_use_to_delete_job = ""
        @redis.lset(failed_queue_name, index_in_failed_queue, hopefully_unique_value_we_can_use_to_delete_job)
        @redis.lrem(failed_queue_name, 1,                     hopefully_unique_value_we_can_use_to_delete_job)
      end

      # Private: do not call
      def list_failed_queue_range(key, start = 0, count = 1)
        if count == 1
          @redis.lindex(key, start)
        else
          Array(@redis.lrange(key, start, start+count-1))
        end
      end
    end

    class Workers
      def initialize(redis)
        @redis = redis
      end

      def worker_ids
        Array(@redis.smembers(:workers))
      end

      # Given a list of worker ids, returns a map of those ids to the worker's value
      # in redis, even if that value maps to nil
      def workers_map(worker_ids)
        redis_keys = worker_ids.map { |id| "worker:#{id}" }
        @redis.mapped_mget(*redis_keys)
      end

      # return the worker's payload i.e. job
      def get_worker_payload(worker_id)
        @redis.get("worker:#{worker_id}")
      end

      def worker_exists?(worker_id)
        @redis.sismember(:workers, worker_id)
      end

      def register_worker(worker)
        @redis.pipelined do |piped|
          piped.sadd(:workers, [worker.id])
          worker_started(worker, redis: piped)
        end
      end

      def worker_started(worker, redis: @redis)
        redis.set(redis_key_for_worker_start_time(worker), Time.now.to_s)
      end

      def unregister_worker(worker, &block)
        @redis.pipelined do |piped|
          piped.srem(:workers, [worker.id])
          piped.del(redis_key_for_worker(worker))
          piped.del(redis_key_for_worker_start_time(worker))
          piped.hdel(HEARTBEAT_KEY, worker.to_s)

          block.call redis: piped
        end
      end

      def remove_heartbeat(worker)
        @redis.hdel(HEARTBEAT_KEY, worker.to_s)
      end

      def heartbeat(worker)
        heartbeat = @redis.hget(HEARTBEAT_KEY, worker.to_s)
        heartbeat && Time.parse(heartbeat)
      end

      def heartbeat!(worker, time)
        @redis.hset(HEARTBEAT_KEY, worker.to_s, time.iso8601)
      end

      def all_heartbeats
        @redis.hgetall(HEARTBEAT_KEY)
      end

      def acquire_pruning_dead_worker_lock(worker, expiry)
        @redis.set(redis_key_for_worker_pruning, worker.to_s, :ex => expiry, :nx => true)
      end

      def set_worker_payload(worker, data)
        @redis.set(redis_key_for_worker(worker), data)
      end

      def worker_start_time(worker)
        @redis.get(redis_key_for_worker_start_time(worker))
      end

      def worker_done_working(worker, &block)
        @redis.pipelined do |piped|
          piped.del(redis_key_for_worker(worker))
          block.call redis: piped
        end
      end

    private

      def redis_key_for_worker(worker)
        "worker:#{worker}"
      end

      def redis_key_for_worker_start_time(worker)
        "#{redis_key_for_worker(worker)}:started"
      end

      def redis_key_for_worker_pruning
        "pruning_dead_workers_in_progress"
      end
    end

    class StatsAccess
      def initialize(redis)
        @redis = redis
      end
      def stat(stat)
        @redis.get("stat:#{stat}").to_i
      end

      def increment_stat(stat, by = 1, redis: @redis)
        redis.incrby("stat:#{stat}", by)
      end

      def decremet_stat(stat, by = 1)
        @redis.decrby("stat:#{stat}", by)
      end

      def clear_stat(stat, redis: @redis)
        redis.del("stat:#{stat}")
      end
    end
  end
end
