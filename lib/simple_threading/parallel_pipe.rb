module SimpleThreading
  module ParallelPipe

    # @param [Array<T>] arr
    # @param [Integer] start_idx
    # @param [Integer] finish_idx
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @return [Array<T>]
    def grouped_pipe(arr, start_idx, finish_idx, opts = {})
      threads = []
      results = []
      close_active_record = opts.fetch(:close_active_record, false)
      for i in start_idx...finish_idx
        threads << Thread.new(arr[i]) do |it|
          Thread.current[:output] = yield it
          ActiveRecord::Base.connection.close if close_active_record
        end
      end
      threads.each do |t|
        t.join
        results << t[:output] if t[:output]
      end
      results
    end

    # @param [Array<T>] arr
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @return [Array<T>]
    def direct_pipe(arr, opts = {})
      threads = []
      results = []
      close_active_record = opts.fetch(:close_active_record, false)
      arr.each do |item|
        threads << Thread.new(item) do |it|
          Thread.current[:output] = yield it
          ActiveRecord::Base.connection.close if close_active_record
        end
      end
      threads.each do |t|
        t.join
        results << t[:output] if t[:output]
      end
      results
    end

    # @param [Array<T>] arr
    # @yield [item] item of arr
    # @return [Array<T>]
    def conditional_map(arr, &block)
      results = []
      arr.each do |item|
        output = block.call item
        results << output if output
      end
      results
    end

    # @param [Array<T>] arr
    # @param [Integer] group_size
    # @param [Hash] opts
    # @option opts [Boolean] :close_active_record
    # @yield [item] item of arr
    # @return [Array<T>]
    def parallel_pipe(arr, group_size, opts = {}, &block)
      if group_size <= 1
        conditional_map arr, &block
      else
        if arr.length <= group_size
          direct_pipe arr, opts, &block
        else
          start_idx = 0
          finish_idx = 0
          results = []
          # Process by chunks
          while( finish_idx < arr.length ) do
            start_idx = finish_idx
            finish_idx += group_size
            if finish_idx > arr.length
              finish_idx = arr.length
            end
            results+= grouped_pipe(arr, start_idx, finish_idx, opts, &block)
          end
          results
        end
      end
    end

  end
end