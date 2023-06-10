module ResqueSqs
  class Railtie < Rails::Railtie
    rake_tasks do
      require 'resque_sqs/tasks'

      # redefine ths task to load the rails env
      task "resque_sqs:setup" => :environment
    end
  end
end
