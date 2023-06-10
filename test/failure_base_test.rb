require 'test_helper'
require 'minitest/mock'

require 'resque_sqs/failure/base'

class TestFailure < ResqueSqs::Failure::Base
end

describe "Base failure class" do
  it "allows calling all without throwing" do
    with_failure_backend TestFailure do
      assert_empty ResqueSqs::Failure.all
    end
  end
end
