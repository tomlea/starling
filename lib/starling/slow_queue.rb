require 'starling/persistent_queue'

module StarlingServer
  class SlowQueue
    extend Forwardable
    RECORD_FORMAT="Ga*"

    def_delegators :@out, :empty?, :length, :num_waiting, :size, :pop, :initial_bytes

    def initialize(persistence_path, queue_name, wait_time, debug = false)
      @in = PersistentQueue.new(persistence_path, "#{queue_name}_in", debug)
      @out = PersistentQueue.new(persistence_path, "#{queue_name}_out", debug)
      @waiting = PersistentQueue.new(persistence_path, "#{queue_name}_waiting", debug)
      @worker = Thread.new(&method(:worker))
      @wait_time = wait_time.to_f
      ObjectSpace.define_finalizer(self, lambda{@worker.terminate})
    end

    def close
      @in.close
      @out.close
      @waiting.close
    end

    def clear
      @in.clear
      @out.clear
      @waiting.clear
    end

    def now
      Time.now.to_f
    end

    def push(data)
      due_time = @wait_time + now
      item = build_data(due_time, data)
      @in.push(item)
    end

    def pop
      @out.pop
    end

    def worker
      wait_time = nil
      while item = @waiting.try_pop || @in.pop
        begin
          time, data = extract_data(item)
          wait_time = time - now
          if wait_time <= 0
            @out.push data
            item, time, data, wait_time = nil
            next
          end
        ensure
          @waiting.push item if item
        end
        sleep wait_time if wait_time
      end
    end

    def extract_data(msg)
      msg.unpack(RECORD_FORMAT)
    end

    def build_data(time, data)
      [time.to_f, data].pack(RECORD_FORMAT)
    end

    alias shift pop
    alias deq pop
    alias << push
    alias enq push
  end
end
