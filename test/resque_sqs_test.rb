require 'test_helper'

describe "Resque" do
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
    ResqueSqs.data_store.sqs.add_queue(:test)
    ResqueSqs.push(:test, JsonObject.new)
    ResqueSqs.redis.del("count")
  end

  it "can set a namespace through a url-like string" do
    assert ResqueSqs.redis
    assert_equal :resque, ResqueSqs.redis.namespace
    ResqueSqs.data_store = ResqueSqs::DataStore.new('localhost:9736/namespace', MockSQSClient.new, 'mock-aws-account-id')
    assert_equal 'namespace', ResqueSqs.redis.namespace
  end

  it "redis= works correctly with a Redis::Namespace param" do
    new_redis = Redis.new(:host => "localhost", :port => 9736)
    new_namespace = Redis::Namespace.new("namespace", :redis => new_redis)
    ResqueSqs.data_store = ResqueSqs::DataStore.new(new_namespace, MockSQSClient.new(['default']), 'mock-aws-account-id')

    assert_equal new_namespace._client, ResqueSqs.redis._client
    assert_equal 0, ResqueSqs.size(:default)
  end

  it "can put jobs on a queue" do
    ResqueSqs.data_store.sqs.add_queue(:jobs)
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
  end

  it "can grab jobs off a queue" do
    ResqueSqs.data_store.sqs.add_queue(:jobs)
    ResqueSqs::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = ResqueSqs.reserve(:jobs)

    assert_kind_of ResqueSqs::Job, job
    assert_equal SomeJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
  end

  it "can re-queue jobs" do
    ResqueSqs.data_store.sqs.add_queue(:jobs)
    ResqueSqs::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = ResqueSqs.reserve(:jobs)
    job.recreate

    assert_equal job, ResqueSqs.reserve(:jobs)
  end

  it "can put jobs on a queue by way of an ivar" do
    ResqueSqs.data_store.sqs.add_queue(:ivar)
    assert_equal 0, ResqueSqs.size(:ivar)
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')

    job = ResqueSqs.reserve(:ivar)

    assert_kind_of ResqueSqs::Job, job
    assert_equal SomeIvarJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert ResqueSqs.reserve(:ivar)
    assert_nil ResqueSqs.reserve(:ivar)
  end

  it "jobs have a nice #inspect" do
    ResqueSqs.data_store.sqs.add_queue(:jobs)
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    job = ResqueSqs.reserve(:jobs)
    assert_equal '(Job{jobs} | SomeJob | [20, "/tmp"])', job.inspect
  end

  it "jobs can it for equality" do
    ResqueSqs.data_store.sqs.add_queue(:jobs)
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'some-job', 20, '/tmp')
    assert_equal ResqueSqs.reserve(:jobs), ResqueSqs.reserve(:jobs)

    assert ResqueSqs::Job.create(:jobs, 'SomeMethodJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    refute_equal ResqueSqs.reserve(:jobs), ResqueSqs.reserve(:jobs)

    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 30, '/tmp')
    refute_equal ResqueSqs.reserve(:jobs), ResqueSqs.reserve(:jobs)
  end

  it "can put jobs on a queue by way of a method" do
    ResqueSqs.data_store.sqs.add_queue(:method)
    assert_equal 0, ResqueSqs.size(:method)
    assert ResqueSqs.enqueue(SomeMethodJob, 20, '/tmp')
    assert ResqueSqs.enqueue(SomeMethodJob, 20, '/tmp')

    job = ResqueSqs.reserve(:method)

    assert_kind_of ResqueSqs::Job, job
    assert_equal SomeMethodJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]

    assert ResqueSqs.reserve(:method)
    assert_nil ResqueSqs.reserve(:method)
  end

  it "can define a queue for jobs by way of a method" do
    ResqueSqs.data_store.sqs.add_queue(:method)
    ResqueSqs.data_store.sqs.add_queue(:new_queue)
    assert_equal 0, ResqueSqs.size(:method)
    assert ResqueSqs.enqueue_to(:new_queue, SomeMethodJob, 20, '/tmp')

    job = ResqueSqs.reserve(:new_queue)
    assert_equal SomeMethodJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
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
    ResqueSqs.data_store.sqs.add_queue(:people)
    assert ResqueSqs.push(:people, { 'name' => 'jon' })
  end

  it "queues are always a list" do
    assert ResqueSqs.queues.is_a?(Array)
  end

  it "sqs queue names get formatted correctly" do
    queue = ResqueSqs.format_queue_name('ResqueSqs-test')

    # puts queue
    # raise queue
    assert "https://sqs.us-east-1.amazonaws.com/mockawsaccountid/ResqueSqs-test" == queue
  end

  it "badly wants a class name, too" do
    assert_raises ResqueSqs::NoClassError do
      ResqueSqs::Job.create(:jobs, nil)
    end
  end

  it "decode bad json" do
    assert_raises ResqueSqs::Helpers::DecodeException do
      ResqueSqs.decode("{\"error\":\"Module not found \\u002\"}")
    end
  end

  it "inlining jobs" do
    ResqueSqs.data_store.sqs.add_queue(:ivar)
    begin
      ResqueSqs.inline = true
      ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
      assert_equal 0, ResqueSqs.size(:ivar)
    ensure
      ResqueSqs.inline = false
    end
  end

  describe "with people in the queue" do
    before do
      ResqueSqs.data_store.sqs.add_queue(:people)
      ResqueSqs.push(:people, { 'name' => 'chris' })
      ResqueSqs.push(:people, { 'name' => 'bob' })
      ResqueSqs.push(:people, { 'name' => 'mark' })
    end

    it "can pull items off a queue" do
      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'chris' }, body)
      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'bob' }, body)
      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'mark' }, body)
      _, body = ResqueSqs.pop(:people)
      assert_nil body
    end

    it "knows how big a queue is" do
      assert_equal 3, ResqueSqs.size(:people)

      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'chris' }, body)
      assert_equal 2, ResqueSqs.size(:people)
      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'bob' }, body)
      _, body = ResqueSqs.pop(:people)
      assert_equal({ 'name' => 'mark' }, body)
      assert_equal 0, ResqueSqs.size(:people)
    end

    it "keeps stats" do
      ResqueSqs.data_store.sqs.purge_all_queues
      ResqueSqs.data_store.sqs.add_queue(:jobs)
      ResqueSqs::Job.create(:jobs, SomeJob, 20, '/tmp')
      ResqueSqs::Job.create(:jobs, BadJob)
      ResqueSqs::Job.create(:jobs, GoodJob)

      ResqueSqs.data_store.sqs.add_queue(:others)
      ResqueSqs::Job.create(:others, GoodJob)
      ResqueSqs::Job.create(:others, GoodJob)

      stats = ResqueSqs.info
      assert_equal 5, stats[:pending]

      @worker = ResqueSqs::Worker.new(:jobs)
      @worker.register_worker
      2.times { @worker.process }

      job = @worker.reserve
      @worker.working_on job

      stats = ResqueSqs.info
      assert_equal 1, stats[:working]
      assert_equal 1, stats[:workers]

      @worker.done_working

      stats = ResqueSqs.info
      assert_equal 2, stats[:queues]
      assert_equal 3, stats[:processed]
      assert_equal 1, stats[:failed]
      assert_equal [ResqueSqs.redis_id], stats[:servers]
    end

    it "keeps stats" do
      ResqueSqs.data_store.sqs.purge_all_queues
      ResqueSqs.data_store.sqs.add_queue(:jobs)
      ResqueSqs::Job.create(:jobs, SomeJob, 20, '/tmp')
      ResqueSqs::Job.create(:jobs, BadJob)
      ResqueSqs::Job.create(:jobs, GoodJob)

      ResqueSqs.data_store.sqs.add_queue(:others)
      ResqueSqs::Job.create(:others, GoodJob)
      ResqueSqs::Job.create(:others, GoodJob)

      stats = ResqueSqs.info
      assert_equal 5, stats[:pending]

      @worker = ResqueSqs::Worker.new(:jobs)
      @worker.register_worker
      2.times { @worker.process }

      @worker.reserve_many(1) do |job|
        @worker.working_on job

        stats = ResqueSqs.info
        assert_equal 1, stats[:working]
        assert_equal 1, stats[:workers]

        @worker.done_working
      end

      stats = ResqueSqs.info
      assert_equal 2, stats[:queues]
      assert_equal 3, stats[:processed]
      assert_equal 1, stats[:failed]
      assert_equal [ResqueSqs.redis_id], stats[:servers]
    end

  end

  describe "stats" do
    it "allows to set custom stat_data_store" do
      dummy = Object.new
      ResqueSqs.stat_data_store = dummy
      assert_equal dummy, ResqueSqs.stat_data_store
    end

    it "queue_sizes with one queue" do
      ResqueSqs.data_store.sqs.purge_all_queues
      ResqueSqs.data_store.sqs.add_queue(:queue1)
      ResqueSqs.enqueue_to(:queue1, SomeJob)

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 1 }, queue_sizes)
    end

    it "queue_sizes with two queue" do
      ResqueSqs.data_store.sqs.purge_all_queues
      ResqueSqs.data_store.sqs.add_queue(:queue1)
      ResqueSqs.data_store.sqs.add_queue(:queue2)
      ResqueSqs.enqueue_to(:queue1, SomeJob)
      ResqueSqs.enqueue_to(:queue2, SomeJob)

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 1, "queue2" => 1, }, queue_sizes)
    end

    it "queue_sizes with two queue with multiple jobs" do
      ResqueSqs.data_store.sqs.purge_all_queues
      ResqueSqs.data_store.sqs.add_queue(:queue1)
      ResqueSqs.data_store.sqs.add_queue(:queue2)
      5.times { ResqueSqs.enqueue_to(:queue1, SomeJob) }
      9.times { ResqueSqs.enqueue_to(:queue2, SomeJob) }

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 5, "queue2" => 9 }, queue_sizes)
    end
  end
end
