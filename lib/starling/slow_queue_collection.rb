require 'starling/slow_queue'
require 'starling/queue_collection'
module StarlingServer
  class SlowQueueCollection < QueueCollection
    def initialize(path, wait_time)
      @wait_time = wait_time
      super(path)
    end

    def new_queue(key)
      SlowQueue.new(@path, key, @wait_time)
    end
  end
end
