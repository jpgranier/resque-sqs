require 'sinatra/base'
require 'resque_sqs'
require 'job'

module Demo
  class App < Sinatra::Base
    get '/' do
      info = ResqueSqs.info
      out = "<html><head><title>Resque Demo</title></head><body>"
      out << "<p>"
      out << "There are #{info[:pending]} pending and "
      out << "#{info[:processed]} processed jobs across #{info[:queues]} queues."
      out << "</p>"
      out << '<form method="POST">'
      out << '<input type="submit" value="Create New Job"/>'
      out << '&nbsp;&nbsp;<a href="/resque/">View Resque</a>'
      out << '</form>'

       out << "<form action='/failing' method='POST''>"
       out << '<input type="submit" value="Create Failing New Job"/>'
       out << '&nbsp;&nbsp;<a href="/resque/">View Resque</a>'
       out << '</form>'

      out << "</body></html>"
      out
    end

    post '/' do
      ResqueSqs.enqueue(Job, params)
      redirect "/"
    end

    post '/failing' do
      ResqueSqs.enqueue(FailingJob, params)
      redirect "/"
    end
  end
end
