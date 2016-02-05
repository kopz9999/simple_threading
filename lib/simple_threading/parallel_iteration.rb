module SimpleThreading
  module ParallelIteration

    # @param [Array<T>] arr
    # @param [Integer] start_idx
    # @param [Integer] finish_idx
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @yield [item] item of arr
    # @return [Array<Thread>]
    def grouped_iteration(arr, start_idx, finish_idx, opts = {})
      threads = []
      close_active_record = opts.fetch(:close_active_record, false)
      for i in start_idx...finish_idx
        threads << Thread.new(arr[i]) do |it|
          yield it
          ActiveRecord::Base.connection.close if close_active_record
        end
      end
      threads.each{|t| t.join }
    end

    # @param [Array<T>] arr
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @yield [item] item of arr
    # @return [Array<Thread>]
    def parallel_each(arr, opts = {})
      threads = []
      close_active_record = opts.fetch(:close_active_record, false)
      arr.each do |item|
        threads << Thread.new(item) do |it|
          yield it
          ActiveRecord::Base.connection.close if close_active_record
        end
      end
      threads.each{|t| t.join }
    end

    # @param [Array<T>] arr
    # @param [Integer] group_size
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @yield [item] item of arr
    # @return [Array<Thread>]
    def parallel_iteration(arr, group_size, opts = {}, &block)
      if group_size <= 1
        arr.each &block
      else
        if arr.length <= group_size
          parallel_each arr, opts, &block
        else
          start_idx = 0
          finish_idx = 0
          # Process by chunks
          while( finish_idx < arr.length ) do
            start_idx = finish_idx
            finish_idx += group_size
            if finish_idx > arr.length
              finish_idx = arr.length
            end
            grouped_iteration(arr, start_idx, finish_idx, opts, &block)
          end
        end
      end
    end

  end
end