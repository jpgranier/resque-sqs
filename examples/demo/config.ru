#!/usr/bin/env ruby
require 'logger'
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../../lib'
$LOAD_PATH.unshift File.dirname(__FILE__) unless $LOAD_PATH.include?(File.dirname(__FILE__))
require 'app'
require 'resque_sqs/server'

use Rack::ShowExceptions

# Set the AUTH env variable to your basic auth password to protect ResqueSqs.
AUTH_PASSWORD = ENV['AUTH']
if AUTH_PASSWORD
  ResqueSqs::Server.use Rack::Auth::Basic do |username, password|
    password == AUTH_PASSWORD
  end
end

run Rack::URLMap.new \
  "/"       => Demo::App.new,
  "/resque" => ResqueSqs::Server.new
