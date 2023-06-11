require 'test_helper'
require 'resque_sqs/failure/multiple'

describe 'ResqueSqs::Failure::Multiple' do
  it 'requeue_all and does not raise an exception' do
    with_failure_backend(ResqueSqs::Failure::Multiple) do
      ResqueSqs::Failure::Multiple.classes = [ResqueSqs::Failure::Redis]
      exception = StandardError.exception('some error')
      ResqueSqs.data_store.sqs.add_queue(:test)
      worker = ResqueSqs::Worker.new(:test)
      payload = { 'class' => 'Object', 'args' => 3 }
      ResqueSqs.data_store.sqs.add_queue(:queue)
      ResqueSqs::Failure.create({:exception => exception, :worker => worker, :queue => 'queue', :payload => payload})
      ResqueSqs::Failure::Multiple.requeue_all # should not raise an error
    end
  end

  it 'requeue_queue delegates to the first class and returns a mapped queue name' do
    with_failure_backend(ResqueSqs::Failure::Multiple) do
      mock_class = MiniTest::Mock.new
      ResqueSqs.data_store.sqs.add_queue(:requeue_queue)
      mock_class.expect(:requeue_queue, 'mapped_queue', ['queue'])
      ResqueSqs::Failure::Multiple.classes = [mock_class]
      assert_equal 'mapped_queue', ResqueSqs::Failure::Multiple.requeue_queue('queue')
    end
  end

  it 'remove passes the queue on to its backend' do
    with_failure_backend(ResqueSqs::Failure::Multiple) do
      mock = Object.new
      def mock.remove(_id, queue)
        @queue = queue
      end

      ResqueSqs::Failure::Multiple.classes = [mock]
      ResqueSqs::Failure::Multiple.remove(1, :test_queue)
      ResqueSqs.data_store.sqs.add_queue(:test_queue)
      assert_equal :test_queue, mock.instance_variable_get('@queue')
    end
  end
end
