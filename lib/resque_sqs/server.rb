require 'sinatra/base'
require 'tilt/erb'
require 'resque_sqs'
require 'resque_sqs/server_helper'
require 'resque_sqs/version'
require 'time'
require 'yaml'

if defined?(Encoding) && Encoding.default_external != Encoding::UTF_8
  Encoding.default_external = Encoding::UTF_8
end

module ResqueSqs
  class Server < Sinatra::Base
    dir = File.dirname(File.expand_path(__FILE__))

    set :views,  "#{dir}/server/views"

    if respond_to? :public_folder
      set :public_folder, "#{dir}/server/public"
    else
      set :public, "#{dir}/server/public"
    end

    set :static, true

    helpers do
      include ResqueSqs::ServerHelper
    end

    def show(page, layout = true)
      response["Cache-Control"] = "max-age=0, private, must-revalidate"
      begin
        erb page.to_sym, {:layout => layout}, :resque => ResqueSqs
      rescue Errno::ECONNREFUSED
        erb :error, {:layout => false}, :error => "Can't connect to Redis! (#{ResqueSqs.redis_id})"
      end
    end

    def show_for_polling(page)
      content_type "text/html"
      @polling = true
      show(page.to_sym, false).gsub(/\s{1,}/, ' ')
    end

    # to make things easier on ourselves
    get "/?" do
      redirect url_path(:overview)
    end

    %w( overview workers ).each do |page|
      get "/#{page}.poll/?" do
        show_for_polling(page)
      end

      get "/#{page}/:id.poll/?" do
        show_for_polling(page)
      end
    end

    %w( overview queues working workers key ).each do |page|
      get "/#{page}/?" do
        show page
      end

      get "/#{page}/:id/?" do
        show page
      end
    end

    post "/queues/:id/remove" do
      ResqueSqs.remove_queue(params[:id])
      redirect u('queues')
    end

    get "/failed/?" do
      if ResqueSqs::Failure.url
        redirect ResqueSqs::Failure.url
      else
        show :failed
      end
    end

    get "/failed/:queue" do
      if ResqueSqs::Failure.url
        redirect ResqueSqs::Failure.url
      else
        show :failed
      end
    end

    post "/failed/clear" do
      ResqueSqs::Failure.clear
      redirect u('failed')
    end

    post "/failed/clear_retried" do
      ResqueSqs::Failure.clear_retried
      redirect u('failed')
    end

    post "/failed/:queue/clear" do
      ResqueSqs::Failure.clear params[:queue]
      redirect u('failed')
    end

    post "/failed/requeue/all" do
      ResqueSqs::Failure.requeue_all
      redirect u('failed')
    end

    post "/failed/:queue/requeue/all" do
      ResqueSqs::Failure.requeue_queue ResqueSqs::Failure.job_queue_name(params[:queue])
      redirect url_path("/failed/#{params[:queue]}")
    end

    get "/failed/requeue/:index/?" do
      ResqueSqs::Failure.requeue(params[:index])
      if request.xhr?
        return ResqueSqs::Failure.all(params[:index])['retried_at']
      else
        redirect u('failed')
      end
    end

    get "/failed/:queue/requeue/:index/?" do
      ResqueSqs::Failure.requeue(params[:index], params[:queue])
      if request.xhr?
        return ResqueSqs::Failure.all(params[:index],1,params[:queue])['retried_at']
      else
        redirect url_path("/failed/#{params[:queue]}")
      end
    end

    get "/failed/remove/:index/?" do
      ResqueSqs::Failure.remove(params[:index])
      redirect u('failed')
    end

    get "/failed/:queue/remove/:index/?" do
      ResqueSqs::Failure.remove(params[:index], params[:queue])
      redirect url_path("/failed/#{params[:queue]}")
    end

    get "/stats/?" do
      redirect url_path("/stats/resque")
    end

    get "/stats/:id/?" do
      show :stats
    end

    get "/stats/keys/:key/?" do
      show :stats
    end

    get "/stats.txt/?" do
      info = ResqueSqs.info

      stats = []
      stats << "resque.pending=#{info[:pending]}"
      stats << "resque.processed+=#{info[:processed]}"
      stats << "resque.failed+=#{info[:failed]}"
      stats << "resque.workers=#{info[:workers]}"
      stats << "resque.working=#{info[:working]}"

      ResqueSqs.queues.each do |queue|
        stats << "queues.#{queue}=#{ResqueSqs.size(queue)}"
      end

      content_type 'text/html'
      stats.join "\n"
    end

    def resque
      ResqueSqs
    end

    def self.tabs
      @tabs ||= ["Overview", "Working", "Failed", "Queues", "Workers", "Stats"]
    end

    def self.url_prefix=(url_prefix)
      @url_prefix = url_prefix
    end

    def self.url_prefix
      (@url_prefix.nil? || @url_prefix.empty?) ? '' : @url_prefix + '/'
    end
  end
end
