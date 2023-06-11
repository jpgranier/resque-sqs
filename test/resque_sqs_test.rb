require 'test_helper'

describe "Resque" do
  SQS_CLIENT = Aws::SQS::Client.new

  before do
    @original_data_store = ResqueSqs.data_store
    @original_stat_data_store = ResqueSqs.stat_data_store
  end

  after do
    ResqueSqs.data_store = @original_data_store
    ResqueSqs.stat_data_store = @original_stat_data_store
  end

  it "can push an item that depends on redis for encoding" do
    ResqueSqs.redis.set("count", 1)
    # No error should be raised
    ResqueSqs.push(QUEUE, JsonObject.new)
    ResqueSqs.redis.del("count")
  end

  it "can delete an item via ResqueSqs" do
    ResqueSqs.redis.set("count", 1)
    # No error should be raised
    ResqueSqs.push(QUEUE, JsonObject.new)
    sleep(1)
    job = ResqueSqs.reserve(QUEUE)
    ResqueSqs.remove_from_queue(QUEUE, job.receipt_handle)
    ResqueSqs.redis.del("count")
  end

  it "can delete an item via Job" do
    ResqueSqs.redis.set("count", 1)
    # No error should be raised
    ResqueSqs.push(QUEUE, JsonObject.new)
    sleep(1)
    job = ResqueSqs.reserve(QUEUE)
    job.remove_from_queue
    ResqueSqs.redis.del("count")
  end

  it "can call delete_from_queue without a receipt_handle" do
    ResqueSqs.redis.set("count", 1)
    # No error should be raised
    job = ResqueSqs::Job.new(QUEUE, JsonObject.new, nil)
    job.remove_from_queue
    ResqueSqs.redis.del("count")
  end

  it "can get queue_size" do
    size = ResqueSqs.size(QUEUE)
    assert size.is_a?(Integer) && size >= 0
  end

  it "can set a namespace through a url-like string" do
    assert ResqueSqs.redis
    assert_equal :resque, ResqueSqs.redis.namespace
    ResqueSqs.data_store = ResqueSqs::DataStore.new('localhost:9736/namespace', SQS_CLIENT)
    assert_equal 'namespace', ResqueSqs.redis.namespace
  end

  it "redis= works correctly with a Redis::Namespace param" do
    new_redis = Redis.new(:host => "localhost", :port => 9736)
    new_namespace = Redis::Namespace.new("namespace", :redis => new_redis)
    ResqueSqs.data_store = ResqueSqs::DataStore.new(new_namespace, SQS_CLIENT)

    assert_equal new_namespace._client, ResqueSqs.redis._client
  end

  it "can put jobs on a queue" do
    assert ResqueSqs::Job.create(QUEUE, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(QUEUE, 'SomeJob', 20, '/tmp')
  end

  it "can grab jobs off a queue" do
    ResqueSqs::Job.create(QUEUE, 'some-job', 20, '/tmp')

    sleep(1)
    assert ResqueSqs.reserve(QUEUE)
  end

  it "can re-queue jobs" do
    ResqueSqs::Job.create(QUEUE, 'some-job', 20, '/tmp')

    sleep(1)
    job = ResqueSqs.reserve(QUEUE)
    job.recreate
  end

  it "can put jobs on a queue by way of an ivar" do
    # assert_equal 0, ResqueSqs.size(:ivar)
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
  end

  it "jobs have a nice #inspect" do
    job = ResqueSqs::Job.new(:jobs, {'class' => 'SomeJob', 'args' => [20, '/tmp']}, nil)
    assert_equal '(Job{jobs} | SomeJob | [20, "/tmp"])', job.inspect
  end

  it "jobs can it for equality" do
    job1 = ResqueSqs::Job.new(QUEUE, {'class' => 'SomeJob', 'args' => [20, '/tmp']}, nil)
    job2 = ResqueSqs::Job.new(QUEUE, {'class' => 'some-job', 'args' => [20, '/tmp']}, nil)
    assert_equal job1, job2

    job1 = ResqueSqs::Job.new(QUEUE, {'class' => 'SomeMethodJob', 'args' => [20, '/tmp']}, nil)
    job2 = ResqueSqs::Job.new(QUEUE, {'class' => 'SomeJob', 'args' => [20, '/tmp']}, nil)
    refute_equal job1, job2

    job1 = ResqueSqs::Job.new(QUEUE, {'class' => 'SomeJob', 'args' => [20, '/tmp']}, nil)
    job2 = ResqueSqs::Job.new(QUEUE, {'class' => 'SomeJob', 'args' => [30, '/tmp']}, nil)
    refute_equal job1, job2
  end

  it "can put jobs on a queue by way of a method" do
    assert ResqueSqs.enqueue(SomeMethodJob, 20, '/tmp')
    assert ResqueSqs.enqueue(SomeMethodJob, 20, '/tmp')
  end

  it "can define a queue for jobs by way of a method" do
    assert ResqueSqs.enqueue_to(QUEUE, SomeMethodJob, 20, '/tmp')
  end

  it "needs to infer a queue with enqueue" do
    assert_raises ResqueSqs::NoQueueError do
      ResqueSqs.enqueue(SomeJob, 20, '/tmp')
    end
  end

  it "validates job for queue presence" do
    err = assert_raises ResqueSqs::NoQueueError do
      ResqueSqs.validate(SomeJob)
    end
    assert_match(/SomeJob/, err.message)
  end

  it "can put items on a queue" do
    assert ResqueSqs.push(QUEUE, { 'name' => 'jon' })
  end

  it "queues are always a list" do
    assert ResqueSqs.queues.is_a?(Array)
  end

  it "badly wants a class name, too" do
    assert_raises ResqueSqs::NoClassError do
      ResqueSqs::Job.create(QUEUE, nil)
    end
  end

  it "decode bad json" do
    assert_raises ResqueSqs::Helpers::DecodeException do
      ResqueSqs.decode("{\"error\":\"Module not found \\u002\"}")
    end
  end

  it "inlining jobs" do
    begin
      ResqueSqs.inline = true
      ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    ensure
      ResqueSqs.inline = false
    end
  end

  describe "stats" do
    it "allows to set custom stat_data_store" do
      dummy = Object.new
      ResqueSqs.stat_data_store = dummy
      assert_equal dummy, ResqueSqs.stat_data_store
    end
  end
end
