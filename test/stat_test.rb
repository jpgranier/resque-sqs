require 'test_helper'

describe "ResqueSqs::Stat" do
  class DummyStatStore
    def initialize
      @stat = Hash.new(0)
    end

    def stat(stat)
      @stat[stat]
    end

    def increment_stat(stat, by = 1, redis: nil)
      @stat[stat] += by
    end

    def decrement_stat(stat, by)
      @stat[stat] -= by
    end

    def clear_stat(stat, redis: nil)
      @stat[stat] = 0
    end
  end

  before do
    @original_data_store = ResqueSqs::Stat.data_store
    @dummy_data_store = DummyStatStore.new
    ResqueSqs::Stat.data_store = @dummy_data_store
  end

  after do
    ResqueSqs::Stat.data_store = @original_data_store
  end

  it '#redis show deprecation warning' do
    assert_output(nil, /\[Resque\] \[Deprecation\]/ ) do
      assert_equal @dummy_data_store, ResqueSqs::Stat.redis
    end
  end

  it '#redis returns data_store' do
    assert_equal @dummy_data_store, ResqueSqs::Stat.data_store
  end

  it "#get" do
    assert_equal 0, ResqueSqs::Stat.get('hello')
  end

  it "#[]" do
    assert_equal 0, ResqueSqs::Stat['hello']
  end

  it "#incr" do
    assert_equal 2, ResqueSqs::Stat.incr('hello', 2)
    assert_equal 2, ResqueSqs::Stat['hello']
  end

  it "#<<" do
    assert_equal 1, ResqueSqs::Stat << 'hello'
    assert_equal 1, ResqueSqs::Stat['hello']
  end

  it "#decr" do
    assert_equal 2, ResqueSqs::Stat.incr('hello', 2)
    assert_equal 2, ResqueSqs::Stat['hello']

    assert_equal 0, ResqueSqs::Stat.decr('hello', 2)
    assert_equal 0, ResqueSqs::Stat['hello']
  end

  it "#>>" do
    assert_equal 2, ResqueSqs::Stat.incr('hello', 2)
    assert_equal 2, ResqueSqs::Stat['hello']

    assert_equal 1, ResqueSqs::Stat >> 'hello'
    assert_equal 1, ResqueSqs::Stat['hello']
  end

  it '#clear' do
    assert_equal 2, ResqueSqs::Stat.incr('hello', 2)
    assert_equal 2, ResqueSqs::Stat['hello']

    ResqueSqs::Stat.clear('hello')

    assert_equal 0, ResqueSqs::Stat['hello']
  end
end
