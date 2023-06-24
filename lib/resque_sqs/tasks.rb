# require 'resque_sqs/tasks'
# will give you the resque tasks


namespace :resque_sqs do
  task :setup

  desc "Start a Resque worker"
  task :work => [ :preload, :setup ] do
    require 'resque_sqs'

    begin
      worker = ResqueSqs::Worker.new
    rescue ResqueSqs::NoQueueError
      abort "set QUEUE env var, e.g. $ QUEUE=critical,high rake resque_sqs:work"
    end

    worker.prepare
    worker.log "Starting worker #{worker}"
    worker.work(ENV['INTERVAL'] || 5, ENV['MAX_POLL'] || 10) # interval, will block
  end

  desc "Start multiple Resque workers. Should only be used in dev mode."
  task :workers do
    threads = []

    if ENV['COUNT'].to_i < 1
      abort "set COUNT env var, e.g. $ COUNT=2 rake resque_sqs:workers"
    end

    ENV['COUNT'].to_i.times do
      threads << Thread.new do
        system "rake resque_sqs:work"
      end
    end

    threads.each { |thread| thread.join }
  end

  # Preload app files if this is Rails
  task :preload => :setup do
    if defined?(Rails) && Rails.respond_to?(:application)
      if Rails.application.config.eager_load
        ActiveSupport.run_load_hooks(:before_eager_load, Rails.application)
        Rails.application.config.eager_load_namespaces.each(&:eager_load!)
      end
    end
  end

  namespace :failures do
    desc "Sort the 'failed' queue for the redis_multi_queue failure backend"
    task :sort do
      require 'resque_sqs'
      require 'resque_sqs/failure/redis'

      warn "Sorting #{ResqueSqs::Failure.count} failures..."
      ResqueSqs::Failure.each(0, ResqueSqs::Failure.count) do |_, failure|
        data = ResqueSqs.encode(failure)
        ResqueSqs.redis.rpush(ResqueSqs::Failure.failure_queue_name(failure['queue']), data)
      end
      warn "done!"
    end
  end
end
