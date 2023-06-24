module ResqueSqs
  # A ResqueSqs::Job represents a unit of work. Each job lives on a
  # single queue and has an associated payload object. The payload
  # is a hash with two attributes: `class` and `args`. The `class` is
  # the name of the Ruby class which should be used to run the
  # job. The `args` are an array of arguments which should be passed
  # to the Ruby class's `perform` class-level method.
  #
  # You can manually run a job using this code:
  #
  #   job = ResqueSqs::Job.reserve(:high)
  #   klass = ResqueSqs::Job.constantize(job.payload['class'])
  #   klass.perform(*job.payload['args'])
  class Job
    include Helpers
    extend Helpers

    # Given a Ruby object, returns a string suitable for storage in a
    # queue.
    def encode(object)
      ResqueSqs.encode(object)
    end

    # Given a string, returns a Ruby object.
    def decode(object)
      ResqueSqs.decode(object)
    end

    # Given a Ruby object, returns a string suitable for storage in a
    # queue.
    def self.encode(object)
      ResqueSqs.encode(object)
    end

    # Given a string, returns a Ruby object.
    def self.decode(object)
      ResqueSqs.decode(object)
    end

    # Given a word with dashes, returns a camel cased version of it.
    def classify(dashed_word)
      ResqueSqs.classify(dashed_word)
    end

    # Tries to find a constant with the name specified in the argument string
    def constantize(camel_cased_word)
      ResqueSqs.constantize(camel_cased_word)
    end

    # Raise ResqueSqs::Job::DontPerform from a before_perform hook to
    # abort the job.
    DontPerform = Class.new(StandardError)

    # The worker object which is currently processing this job.
    attr_accessor :worker

    # The name of the queue from which this job was pulled (or is to be
    # placed)
    attr_reader :queue

    # This job's associated payload object.
    attr_reader :payload

    # This job's associated identifier for deletion
    attr_reader :receipt_handle

    def initialize(queue, payload, receipt_handle)
      @queue = queue
      @payload = payload
      @receipt_handle = receipt_handle
      @failure_hooks_ran = false
    end

    # Creates a job by placing it on a queue. Expects a string queue
    # name, a string class name, and an optional array of arguments to
    # pass to the class' `perform` method.
    #
    # Raises an exception if no queue or class is given.
    def self.create(queue, klass, *args)
      ResqueSqs.validate(klass, queue)

      if ResqueSqs.inline?
        # Instantiating a ResqueSqs::Job and calling perform on it so callbacks run
        # decode(encode(args)) to ensure that args are normalized in the same manner as a non-inline job
        new(:inline, {'class' => klass, 'args' => decode(encode(args))}, nil).perform
      else
        ResqueSqs.push(queue, :class => klass.to_s, :args => args)
      end
    end

    # Given a string queue name, returns an instance of ResqueSqs::Job
    # if any jobs are available. If not, returns nil.
    def self.reserve(queue)
      receipt_handle, payload = ResqueSqs.pop(queue)
      return unless payload

      new(queue, payload, receipt_handle)
    end

    # Given a string queue name, yields instances of ResqueSqs::Job
    # if any jobs are available.
    def self.poll(queue, max_poll = 10)
      ResqueSqs.poll(queue, max_poll) do |receipt_handle, payload|
        yield new(queue, payload, receipt_handle)
      end
    end

    # Attempts to perform the work represented by this job instance.
    # Calls #perform on the class given in the payload with the
    # arguments given in the payload.
    def perform
      job = payload_class
      job_args = args || []
      job_was_performed = false

      begin
        # Execute before_perform hook. Abort the job gracefully if
        # ResqueSqs::Job::DontPerform is raised.
        begin
          before_hooks.each do |hook|
            job.send(hook, *job_args)
          end
        rescue DontPerform
          return false
        end

        # Execute the job. Do it in an around_perform hook if available.
        if around_hooks.empty?
          job.perform(*job_args)
          job_was_performed = true
        else
          # We want to nest all around_perform plugins, with the last one
          # finally calling perform
          stack = around_hooks.reverse.inject(nil) do |last_hook, hook|
            if last_hook
              lambda do
                job.send(hook, *job_args) { last_hook.call }
              end
            else
              lambda do
                job.send(hook, *job_args) do
                  result = job.perform(*job_args)
                  job_was_performed = true
                  result
                end
              end
            end
          end
          stack.call
        end

        # Execute after_perform hook
        after_hooks.each do |hook|
          job.send(hook, *job_args)
        end

        # Return true if the job was performed
        return job_was_performed

      # If an exception occurs during the job execution, look for an
      # on_failure hook then re-raise.
      rescue Object => e
        run_failure_hooks(e)
        raise e
      end
    end

    def remove_from_queue
      ResqueSqs.remove_from_queue(@queue, @receipt_handle) unless @receipt_handle.nil?
    end

    # Returns the actual class constant represented in this job's payload.
    def payload_class
      @payload_class ||= constantize(@payload['class'])
    end

    # Returns the payload class as a string without raising NameError
    def payload_class_name
      payload_class.to_s
    rescue NameError
      'No Name'
    end

    def has_payload_class?
      payload_class != Object
    rescue NameError
      false
    end

    # Returns an array of args represented in this job's payload.
    def args
      @payload['args']
    end

    # Given an exception object, hands off the needed parameters to
    # the Failure module.
    def fail(exception)
      begin
        run_failure_hooks(exception)
      rescue Exception => e
        raise e
      ensure
        Failure.create \
          :payload   => payload,
          :exception => exception,
          :worker    => worker,
          :queue     => queue
      end
    end

    # Creates an identical job, essentially placing this job back on
    # the queue.
    def recreate
      self.class.create(queue, payload_class, *args)
    end

    # String representation
    def inspect
      obj = @payload
      "(Job{%s} | %s | %s)" % [ @queue, obj['class'], obj['args'].inspect ]
    end

    # Equality
    def ==(other)
      queue == other.queue &&
        payload_class == other.payload_class &&
        args == other.args
    end

    def before_hooks
      @before_hooks ||= Plugin.before_hooks(payload_class)
    end

    def around_hooks
      @around_hooks ||= Plugin.around_hooks(payload_class)
    end

    def after_hooks
      @after_hooks ||= Plugin.after_hooks(payload_class)
    end

    def failure_hooks
      @failure_hooks ||= Plugin.failure_hooks(payload_class)
    end

    def run_failure_hooks(exception)
      begin
        job_args = args || []
        if has_payload_class?
          failure_hooks.each { |hook| payload_class.send(hook, exception, *job_args) } unless @failure_hooks_ran
        end
      rescue Exception => e
        error_message = "Additional error (#{e.class}: #{e}) occurred in running failure hooks for job #{inspect}\n" \
                        "Original error that caused job failure was #{e.class}: #{exception.class}: #{exception.message}"
        raise RuntimeError.new(error_message)
      ensure
        @failure_hooks_ran = true
      end
    end
  end
end
