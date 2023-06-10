require 'rack/utils'

module ResqueSqs
  module ServerHelper
    include Rack::Utils
    alias_method :h, :escape_html

    def current_section
      url_path request.path_info.sub('/','').split('/')[0].downcase
    end

    def current_page
      url_path request.path_info.sub('/','')
    end

    def url_path(*path_parts)
      [ url_prefix, path_prefix, path_parts ].join("/").squeeze('/')
    end
    alias_method :u, :url_path

    def path_prefix
      request.env['SCRIPT_NAME']
    end

    def class_if_current(path = '')
      'class="current"' if current_page[0, path.size] == path
    end

    def tab(name)
      dname = name.to_s.downcase
      path = url_path(dname)
      "<li #{class_if_current(path)}><a href='#{path.gsub(" ", "_")}'>#{name}</a></li>"
    end

    def tabs
      ResqueSqs::Server.tabs
    end

    def url_prefix
      ResqueSqs::Server.url_prefix
    end

    def redis_get_size(key)
      case ResqueSqs.redis.type(key)
      when 'none'
        0
      when 'hash'
        ResqueSqs.redis.hlen(key)
      when 'list'
        ResqueSqs.redis.llen(key)
      when 'set'
        ResqueSqs.redis.scard(key)
      when 'string'
        ResqueSqs.redis.get(key).length
      when 'zset'
        ResqueSqs.redis.zcard(key)
      end
    end

    def redis_get_value_as_array(key, start=0)
      case ResqueSqs.redis.type(key)
      when 'none'
        []
      when 'hash'
        ResqueSqs.redis.hgetall(key).to_a[start..(start + 20)]
      when 'list'
        ResqueSqs.redis.lrange(key, start, start + 20)
      when 'set'
        ResqueSqs.redis.smembers(key)[start..(start + 20)]
      when 'string'
        [ResqueSqs.redis.get(key)]
      when 'zset'
        ResqueSqs.redis.zrange(key, start, start + 20)
      end
    end

    def show_args(args)
      Array(args).map do |a|
        a.to_yaml
      end.join("\n")
    rescue
      args.to_s
    end

    def worker_hosts
      @worker_hosts ||= worker_hosts!
    end

    def worker_hosts!
      hosts = Hash.new { [] }

      ResqueSqs.workers.each do |worker|
        host, _ = worker.to_s.split(':')
        hosts[host] += [worker.to_s]
      end

      hosts
    end

    def partial?
      @partial
    end

    def partial(template, local_vars = {})
      @partial = true
      erb(template.to_sym, {:layout => false}, local_vars)
    ensure
      @partial = false
    end

    def poll
      if defined?(@polling) && @polling
        text = "Last Updated: #{Time.now.strftime("%H:%M:%S")}"
      else
        text = "<a href='#{u(request.path_info)}.poll' rel='poll'>Live Poll!!</a>"
      end
      "<p class='poll'>#{text}</p>"
    end

    ####################
    #failed.erb helpers#
    ####################

    def failed_date_format
      "%Y/%m/%d %T %z"
    end

    def failed_multiple_queues?
      return @multiple_failed_queues if defined?(@multiple_failed_queues)

      @multiple_failed_queues = ResqueSqs::Failure.queues.size > 1 ||
        (defined?(ResqueSqs::Failure::RedisMultiQueue) && ResqueSqs::Failure.backend == ResqueSqs::Failure::RedisMultiQueue)
    end

    def failed_size
      @failed_size ||= ResqueSqs::Failure.count(params[:queue], params[:class])
    end

    def failed_per_page
      @failed_per_page = if params[:class]
        failed_size
      else
        20
      end
    end

    def failed_start_at
      params[:start].to_i
    end

    def failed_end_at
      if failed_start_at + failed_per_page > failed_size
        failed_size
      else
        failed_start_at  + failed_per_page - 1
      end
    end

    def failed_order
      params[:order] || 'desc'
    end

    def failed_class_counts(queue = params[:queue])
      classes = Hash.new(0)
      ResqueSqs::Failure.each(0, ResqueSqs::Failure.count(queue), queue) do |_, item|
        class_name = item['payload']['class'] if item['payload']
        class_name ||= "nil"
        classes[class_name] += 1
      end
      classes
    end

    def page_entries_info(start, stop, size, name = nil)
      if size == 0
        name ? "No #{name}s" : '<b>0</b>'
      elsif size == 1
        'Showing <b>1</b>' + (name ? " #{name}" : '')
      elsif size > failed_per_page
        "Showing #{start}-#{stop} of <b>#{size}</b>" + (name ? " #{name}s" : '')
      else
        "Showing #{start} to <b>#{size - 1}</b>" + (name ? " #{name}s" : '')
      end
    end
  end
end
