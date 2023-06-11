require 'test_helper'

describe "Resque" do
  before do
    @original_redis = ResqueSqs.redis
    @original_stat_data_store = ResqueSqs.stat_data_store
  end

  after do
    ResqueSqs.redis = @original_redis
    ResqueSqs.stat_data_store = @original_stat_data_store
  end

  it "can push an item that depends on redis for encoding" do
    ResqueSqs.redis.set("count", 1)
    # No error should be raised
    ResqueSqs.push(:test, JsonObject.new)
    ResqueSqs.redis.del("count")
  end

  it "can set a namespace through a url-like string" do
    assert ResqueSqs.redis
    assert_equal :resque, ResqueSqs.redis.namespace
    ResqueSqs.redis = 'localhost:9736/namespace'
    assert_equal 'namespace', ResqueSqs.redis.namespace
  end

  it "redis= works correctly with a Redis::Namespace param" do
    new_redis = Redis.new(:host => "localhost", :port => 9736)
    new_namespace = Redis::Namespace.new("namespace", :redis => new_redis)
    ResqueSqs.redis = new_namespace

    assert_equal new_namespace._client, ResqueSqs.redis._client
    assert_equal 0, ResqueSqs.size(:default)
  end

  it "can put jobs on a queue" do
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
  end

  it "can grab jobs off a queue" do
    ResqueSqs::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = ResqueSqs.reserve(:jobs)

    assert_kind_of ResqueSqs::Job, job
    assert_equal SomeJob, job.payload_class
    assert_equal 20, job.args[0]
    assert_equal '/tmp', job.args[1]
  end

  it "can re-queue jobs" do
    ResqueSqs::Job.create(:jobs, 'some-job', 20, '/tmp')

    job = ResqueSqs.reserve(:jobs)
    job.recreate

    assert_equal job, ResqueSqs.reserve(:jobs)
  end

  it "can put jobs on a queue by way of an ivar" do
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

  it "can remove jobs from a queue by way of an ivar" do
    assert_equal 0, ResqueSqs.size(:ivar)
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    assert ResqueSqs.enqueue(SomeIvarJob, 30, '/tmp')
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    assert ResqueSqs::Job.create(:ivar, 'blah-job', 20, '/tmp')
    assert ResqueSqs.enqueue(SomeIvarJob, 20, '/tmp')
    assert_equal 5, ResqueSqs.size(:ivar)

    assert_equal 1, ResqueSqs.dequeue(SomeIvarJob, 30, '/tmp')
    assert_equal 4, ResqueSqs.size(:ivar)
    assert_equal 3, ResqueSqs.dequeue(SomeIvarJob)
    assert_equal 1, ResqueSqs.size(:ivar)
  end

  it "jobs have a nice #inspect" do
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    job = ResqueSqs.reserve(:jobs)
    assert_equal '(Job{jobs} | SomeJob | [20, "/tmp"])', job.inspect
  end

  it "jobs can be destroyed" do
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'BadJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'SomeJob', 20, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'BadJob', 30, '/tmp')
    assert ResqueSqs::Job.create(:jobs, 'BadJob', 20, '/tmp')

    assert_equal 5, ResqueSqs.size(:jobs)
    assert_equal 2, ResqueSqs::Job.destroy(:jobs, 'SomeJob')
    assert_equal 3, ResqueSqs.size(:jobs)
    assert_equal 1, ResqueSqs::Job.destroy(:jobs, 'BadJob', 30, '/tmp')
    assert_equal 2, ResqueSqs.size(:jobs)
  end

  it "jobs can it for equality" do
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
    assert ResqueSqs.push(:people, { 'name' => 'jon' })
  end

  it "queues are always a list" do
    assert_equal [], ResqueSqs.queues
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
      ResqueSqs.push(:people, { 'name' => 'chris' })
      ResqueSqs.push(:people, { 'name' => 'bob' })
      ResqueSqs.push(:people, { 'name' => 'mark' })
    end

    it "can pull items off a queue" do
      assert_equal({ 'name' => 'chris' }, ResqueSqs.pop(:people).last)
      assert_equal({ 'name' => 'bob' }, ResqueSqs.pop(:people).last)
      assert_equal({ 'name' => 'mark' }, ResqueSqs.pop(:people).last)
      assert_nil ResqueSqs.pop(:people).last
    end

    it "knows how big a queue is" do
      assert_equal 3, ResqueSqs.size(:people)

      assert_equal({ 'name' => 'chris' }, ResqueSqs.pop(:people).last)
      assert_equal 2, ResqueSqs.size(:people)

      assert_equal({ 'name' => 'bob' }, ResqueSqs.pop(:people).last)
      assert_equal({ 'name' => 'mark' }, ResqueSqs.pop(:people).last)
      assert_equal 0, ResqueSqs.size(:people)
    end

    it "can peek at a queue" do
      assert_equal({ 'name' => 'chris' }, ResqueSqs.peek(:people))
      assert_equal 3, ResqueSqs.size(:people)
    end

    it "can peek multiple items on a queue" do
      assert_equal({ 'name' => 'bob' }, ResqueSqs.peek(:people, 1, 1))

      assert_equal([{ 'name' => 'bob' }, { 'name' => 'mark' }], ResqueSqs.peek(:people, 1, 2))
      assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }], ResqueSqs.peek(:people, 0, 2))
      assert_equal([{ 'name' => 'chris' }, { 'name' => 'bob' }, { 'name' => 'mark' }], ResqueSqs.peek(:people, 0, 3))
      assert_equal({ 'name' => 'mark' }, ResqueSqs.peek(:people, 2, 1))
      assert_nil ResqueSqs.peek(:people, 3)
      assert_equal [], ResqueSqs.peek(:people, 3, 2)
    end

    it "can delete a queue" do
      ResqueSqs.push(:cars, { 'make' => 'bmw' })
      assert_equal %w( cars people ).sort, ResqueSqs.queues.sort
      ResqueSqs.remove_queue(:people)
      assert_equal %w( cars ), ResqueSqs.queues
      assert_nil ResqueSqs.pop(:people)
    end

    it "knows what queues it is managing" do
      assert_equal %w( people ), ResqueSqs.queues
      ResqueSqs.push(:cars, { 'make' => 'bmw' })
      assert_equal %w( cars people ).sort, ResqueSqs.queues.sort
    end

    it "keeps track of resque keys" do
      # ignore the heartbeat key that gets set in a background thread
      keys = ResqueSqs.keys - ['workers:heartbeat']

      assert_equal ["queue:people", "queues"].sort, keys.sort
    end

    it "keeps stats" do
      ResqueSqs::Job.create(:jobs, SomeJob, 20, '/tmp')
      ResqueSqs::Job.create(:jobs, BadJob)
      ResqueSqs::Job.create(:jobs, GoodJob)

      ResqueSqs::Job.create(:others, GoodJob)
      ResqueSqs::Job.create(:others, GoodJob)

      stats = ResqueSqs.info
      assert_equal 8, stats[:pending]

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
      assert_equal 3, stats[:queues]
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
      ResqueSqs.enqueue_to(:queue1, SomeJob)

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 1 }, queue_sizes)
    end

    it "queue_sizes with two queue" do
      ResqueSqs.enqueue_to(:queue1, SomeJob)
      ResqueSqs.enqueue_to(:queue2, SomeJob)

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 1, "queue2" => 1, }, queue_sizes)
    end

    it "queue_sizes with two queue with multiple jobs" do
      5.times { ResqueSqs.enqueue_to(:queue1, SomeJob) }
      9.times { ResqueSqs.enqueue_to(:queue2, SomeJob) }

      queue_sizes = ResqueSqs.queue_sizes

      assert_equal({ "queue1" => 5, "queue2" => 9 }, queue_sizes)
    end

    it "sample_queues with simple job with no args" do
      ResqueSqs.enqueue_to(:queue1, SomeJob)
      queues = ResqueSqs.sample_queues

      assert_equal 1, queues.length
      assert_instance_of Hash, queues['queue1']

      assert_equal 1, queues['queue1'][:size]

      samples = queues['queue1'][:samples]
      assert_equal "SomeJob", samples[0]['class']
      assert_equal([], samples[0]['args'])
    end

    it "sample_queues with simple job with args" do
      ResqueSqs.enqueue_to(:queue1, SomeJob, :arg1 => '1')

      queues = ResqueSqs.sample_queues

      assert_equal 1, queues['queue1'][:size]

      samples = queues['queue1'][:samples]
      assert_equal "SomeJob", samples[0]['class']
      assert_equal([{'arg1' => '1'}], samples[0]['args'])
    end

    it "sample_queues with simple jobs" do
      ResqueSqs.enqueue_to(:queue1, SomeJob, :arg1 => '1')
      ResqueSqs.enqueue_to(:queue1, SomeJob, :arg1 => '2')

      queues = ResqueSqs.sample_queues

      assert_equal 2, queues['queue1'][:size]

      samples = queues['queue1'][:samples]
      assert_equal([{'arg1' => '1'}], samples[0]['args'])
      assert_equal([{'arg1' => '2'}], samples[1]['args'])
    end

    it "sample_queues with more jobs only returns sample size number of jobs" do
      11.times { ResqueSqs.enqueue_to(:queue1, SomeJob) }

      queues = ResqueSqs.sample_queues(10)

      assert_equal 11, queues['queue1'][:size]

      samples = queues['queue1'][:samples]
      assert_equal 10, samples.count
    end
  end
end
