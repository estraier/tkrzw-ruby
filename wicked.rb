#! /usr/bin/ruby -w
# -*- coding: utf-8 -*-

require 'optparse'
require 'tkrzw'

include Tkrzw


# main routine
def main
  path = ""
  open_params = {}
  open_params_expr = ""
  num_iterations = 10000
  num_threads = 1
  op = OptionParser.new
  op.on('--path str') { |v| path = v }
  op.on('--params str') { |v| open_params_expr = v }
  op.on('--iter num') { |v| num_iterations = v.to_i }
  op.on('--threads num') { |v| num_threads = v.to_i }
  op.parse(ARGV)
  open_params_expr.split(",").each do |expr|
    columns = expr.split("=", 2)
    if columns.size == 2
      open_params[columns[0]] = columns[1]
    end
  end
  printf("path: %s\n", path)
  printf("params: %s\n", open_params)
  printf("num_iterations: %d\n", num_iterations)
  printf("num_threads: %d\n", num_threads)
  printf("\n")
  open_params["truncate"] = true
  GC.start
  start_mem_usage = Utility.get_memory_usage
  dbm = DBM.new
  dbm.open(path, true, open_params)
  print("Doing:\n")
  start_time = Time.now
  tasks = []
  (0...num_threads).each do |param_thid|
    th = Thread.new(param_thid) do |thid|
      rnd_state = Random.new(thid)
      (0...num_iterations).each do |i|
        key_num = rnd_state.rand(num_iterations) + 1
        key = "%08d" % key_num
        value = "%d" % i
        if rnd_state.rand(num_iterations / 2) == 0
          dbm.rebuild().or_die
        elsif rnd_state.rand(num_iterations / 2) == 0
          dbm.clear().or_die
        elsif rnd_state.rand(num_iterations / 2) == 0
          dbm.synchronize(false).or_die()
        elsif rnd_state.rand(100) == 0
          iter = dbm.make_iterator
          if dbm.ordered? and rnd_state.rand(3) == 0
            if rnd_state.rand(3) == 0
              iter.jump(key)
            else
              iter.last
            end
            while rnd_state.rand(10) == 0 do
              status = Status.new
              iter.get(status)
              if status != Status::NOT_FOUND_ERROR
                status.or_die
              end
              iter.previous
            end
          else
            if rnd_state.rand(3) == 0
              iter.jump(key)
            else
              iter.first
            end
            while rnd_state.rand(10) == 0
              status = Status.new
              iter.get(status)
              if status != Status::NOT_FOUND_ERROR
                status.or_die
              end
              iter.next
            end
          end
        elsif rnd_state.rand(3) == 0
          status = Status.new
          dbm.get(key, status)
          if status != Status::NOT_FOUND_ERROR
            status.or_die
          end
        elsif rnd_state.rand(3) == 0
          status = dbm.remove(key)
          if status != Status::NOT_FOUND_ERROR
            status.or_die()
          end
        elsif rnd_state.rand(3) == 0
          status = dbm.set(key, value, false)
          if status != Status::DUPLICATION_ERROR
            status.or_die
          end
        else
          dbm.set(key, value).or_die
        end
        seq = i + 1
        if thid == 0 and seq % (num_iterations / 500) == 0
          print(".")
          if seq % (num_iterations / 10) == 0
            printf(" (%08d)\n", seq)
          end
        end
      end
    end
    tasks.push(th)
  end
  tasks.each do |th|
    th.join
  end
  dbm.synchronize(false).or_die
  end_time = Time.now
  elapsed = end_time - start_time
  GC.start
  mem_usage = Utility.get_memory_usage - start_mem_usage
  printf("Done: num_records=%d file_size=%d time=%.3f qps=%.0f mem=%d\n",
         dbm.count, (dbm.file_size or -1), elapsed,
         num_iterations * num_threads / elapsed, mem_usage)
  printf("\n")
  dbm.close.or_die
  dbm.destruct
  return 0
end


STDOUT.sync = true
exit(main)


# END OF FILE
