#! /usr/bin/ruby -w
# -*- coding: utf-8 -*-

require 'fileutils'
require 'test/unit'
require 'tkrzw'
require 'tmpdir'

include Tkrzw

class TkrzwTest < Test::Unit::TestCase

  # Prepares resources.
  def setup
    @tmp_dir = Dir.mktmpdir("tkrzw-")
  end

  # Cleanups resources.
  def teardown
    FileUtils.rm_rf(@tmp_dir)
  end

  # Makes a temporary path.
  def _make_tmp_path(name)
    File.join(@tmp_dir, name)
  end

  # Utility tests.
  def test_utility
    assert_match(/^\d+\.\d+\.\d+$/ , Utility::VERSION)
    assert_equal(-2 ** 31, Utility::INT32MIN)
    assert_equal(2 ** 31 - 1, Utility::INT32MAX)
    assert_equal(2 ** 32 - 1, Utility::UINT32MAX)
    assert_equal(-2 ** 63, Utility::INT64MIN)
    assert_equal(2 ** 63 - 1, Utility::INT64MAX)
    assert_equal(2 ** 64 - 1, Utility::UINT64MAX)
    assert_true(Utility.get_memory_usage > 0)
    assert_equal(3042090208, Utility.primary_hash("abc", (1 << 32) - 1))
    assert_equal(16973900370012003622, Utility.primary_hash("abc"))
    assert_equal(702176507, Utility.secondary_hash("abc", (1 << 32) - 1))
    assert_equal(1765794342254572867, Utility.secondary_hash("abc"))
    assert_equal(0, Utility.edit_distance_lev("", ""))
    assert_equal(1, Utility.edit_distance_lev("ac", "abc"))
    assert_equal(3, Utility.edit_distance_lev("あいう", "あう"))
    assert_equal(1, Utility.edit_distance_lev("あいう", "あう", true))
  end

  # Status tests.
  def test_status
    status = Status.new
    assert_equal(Status::SUCCESS, status.code)
    assert_equal(Status::SUCCESS, status)
    assert_not_equal(Status::UNKNOWN_ERROR, status)
    assert_equal("", status.message)
    assert_true(status.ok?)
    status.set(Status::NOT_FOUND_ERROR, "foobar")
    assert_equal("NOT_FOUND_ERROR: foobar", status.to_s)
    assert_false(status.ok?)
    expt = assert_raises StatusException do
      status.or_die
    end
    assert_true(expt.is_a?(StatusException))
    assert_equal("NOT_FOUND_ERROR: foobar", expt.message)
    assert_equal(status, expt.status)
  end

  # Basic tests.
  def test_basic
    confs = [
      {path: "casket.tkh",
       open_params:
         {update_mode: "UPDATE_APPENDING", offset_width: 3, align_pow: 1, num_buckets: 100,
          fbp_capacity: 64, concurrent: true},
       rebuild_params:
         {update_mode: "UPDATE_APPENDING", offset_width: 3, align_pow: 1, num_buckets: 100,
          fbp_capacity: 64},
       synchronize_params: {},
       expected_class: "HashDBM"},
     {path: "casket.tkt",
      open_params:
        {update_mode: "UPDATE_APPENDING", offset_width: 3, align_pow: 1, num_buckets: 100,
         fbp_capacity: 64, max_page_size: 100, max_branches: 4, max_cached_pages: 1,
         key_comparator: "decimal", concurrent: true},
      rebuild_params:
        {update_mode: "UPDATE_APPENDING", offset_width: 3, align_pow: 1, num_buckets: 100,
         fbp_capacity: 64, max_page_size: 100, max_branches: 4, max_cached_pages: 1},
      synchronize_params: {},
      expected_class: "TreeDBM"},
     {path: "casket.tks",
      open_params:
        {offset_width: 3, step_unit: 2, max_level: 3, sort_mem_size: 100,
         insert_in_order: false, max_cached_records: 8, concurrent: true},
      rebuild_params:
        {offset_width: 3, step_unit: 2, max_level: 3, sort_mem_size: 100,
         max_cached_records: 8},
      synchronize_params: {reducer: "last"},
      expected_class: "SkipDBM"},
     {path: "casket.tiny",
      open_params: {num_buckets: 10},
      rebuild_params: {num_buckets: 10},
      synchronize_params: {},
      expected_class: "TinyDBM"},
     {path: "casket.baby",
      open_params: {key_comparator: "decimal"},
      rebuild_params: {},
      synchronize_params: {},
      expected_class: "BabyDBM"},
     {path: "casket.cache",
      open_params: {cap_rec_num: 10000, cap_mem_size: 1000000},
      rebuild_params: {cap_rec_num: 10000},
      synchronize_params: {},
      expected_class: "CacheDBM"},
     {path: "casket.stdhash",
      open_params: {num_buckets: 10},
      rebuild_params: {},
      synchronize_params: {},
      expected_class: "StdHashDBM"},
     {path: "casket.stdtree",
      open_params: {},
      rebuild_params: {},
      synchronize_params: {},
      expected_class: "StdTreeDBM"},
     {path: "casket",
      open_params: {num_shards: 4, dbm: "hash", num_buckets: 100, lock_mem_buckets: false},
      rebuild_params: {},
      synchronize_params: {},
      expected_class: "HashDBM"},
    ]
    confs.each do |conf|
      path = conf[:path]
      path = path.empty? ? path : _make_tmp_path(path)
      dbm = DBM.new
      assert_false(dbm.open?)
      open_params = conf[:open_params].clone
      open_params[:truncate] = true
      open_params[:encoding] = "UTF-8"
      assert_equal(Status::SUCCESS, dbm.open(path, true, open_params))
      inspect = dbm.inspect_details
      class_name = inspect["class"]
      assert_equal(conf[:expected_class], class_name)
      (0...20).each do |i|
        key = "%08d" % i
        value = "%d" % i
        assert_equal(Status::SUCCESS, dbm.set(key, value, false))
      end
      (0...20).step(2) do |i|
        key = "%08d" % i
        assert_equal(Status::SUCCESS, dbm.remove(key))
      end
      assert_equal(Status::SUCCESS, dbm.synchronize(false, conf[:synchronize_params]))
      assert_equal(10, dbm.count)
      assert_true(dbm.file_size > 0)
      if not path.empty?
        dbm.path.include?(path)
      end
      assert_true(dbm.open?)
      assert_true(dbm.healthy?)
      if ["TreeDBM", "SkipDBM", "BabyDBM", "StdTreeDBM"].include?(class_name)
        assert_true(dbm.ordered?)
      else
        assert_false(dbm.ordered?)
      end
      assert_equal(10, dbm.to_i)
      assert_true(dbm.inspect.include?(conf[:expected_class]))
      assert_true(dbm.to_s.include?(conf[:expected_class]))
      (0...20).each do |i|
        key = "%08d" % i
        value = "new-%d" % i
        status = dbm.set(key, value, false)
        if i % 2 == 0
          assert_true(status == Status::SUCCESS)
        else
          assert_true(status == Status::DUPLICATION_ERROR)
        end
      end
      sv = dbm.set_and_get("98765", "apple", false)
      assert_equal(Status::SUCCESS, sv[0])
      assert_equal(nil, sv[1])
      if ["TreeDBM", "TreeDBM", "TinyDBM", "BabyDBM"].include?(class_name)
        sv = dbm.set_and_get("98765", "orange", false)
        assert_equal(Status::DUPLICATION_ERROR, sv[0])
        assert_equal("apple", sv[1])
        sv = dbm.set_and_get("98765", "orange", true)
        assert_equal(Status::SUCCESS, sv[0])
        assert_equal("apple", sv[1])
        assert_equal("orange", dbm.get("98765"))
        sv = dbm.remove_and_get("98765")
        assert_equal(Status::SUCCESS, sv[0])
        assert_equal("orange", sv[1])
        sv = dbm.remove_and_get("98765")
        assert_equal(Status::NOT_FOUND_ERROR, sv[0])
        assert_equal(nil, sv[1])
        assert_equal(Status::SUCCESS, dbm.set("98765", "banana"))
      end
      assert_equal(Status::SUCCESS, dbm.remove("98765"))
      assert_equal(Status::SUCCESS, dbm.synchronize(false, conf[:synchronize_params]))
      records = {}
      (0...20).each do |i|
        key = "%08d" % i
        value = i % 2 == 0 ? "new-%d" % i : "%d" % i
        assert_equal(value, dbm.get(key))
        status = Status.new
        rec_value = dbm.get(key, status)
        assert_equal(value, rec_value)
        assert_equal(Status::SUCCESS, status)
        records[key] = value
      end
      assert_equal(Status::SUCCESS, dbm.rebuild(conf[:rebuild_params]))
      iter_records = {}
      iter = dbm.make_iterator
      status = Status.new
      record = iter.get(status);
      assert_equal(Status::NOT_FOUND_ERROR, status)
      assert_equal(nil, record)
      assert_equal(Status::SUCCESS, iter.first)
      assert_true(iter.inspect.include?("0000"))
      assert_true(iter.to_s.include?("0000"))
      while true do
        status = Status.new
        record = iter.get(status)
        if status != Status::SUCCESS
          assert_equal(Status::NOT_FOUND_ERROR, status)
          break
        end
        assert_equal(2, record.size)
        iter_records[record[0]] = record[1]
        assert_equal(Status::SUCCESS, iter.next)
      end
      assert_equal(records, iter_records)
      iter_records = {}
      dbm.each do |key, value|
        iter_records[key] = value
      end
      assert_equal(records, iter_records)
      if dbm.ordered?
        assert_equal(Status::SUCCESS, iter.last)
        iter_records = {}
        while true do
          status = Status.new
          record = iter.get(status)
          if status != Status::SUCCESS
            assert_equal(Status::NOT_FOUND_ERROR, status)
            break
          end
          assert_equal(2, record.size)
          iter_records[record[0]] = record[1]
          assert_equal(Status::SUCCESS, iter.previous)
        end
        assert_equal(records, iter_records)
      end
      iter.destruct
      if not path.empty?
        dir, base = File.split(path)
        ext = File.extname(base)
        copy_path = File.join(dir, File.basename(path, ext) + "-copy" + ext)
        assert_equal(Status::SUCCESS, dbm.copy_file_data(copy_path))
        copy_dbm = DBM.new
        if path.index(".")
          assert_equal(Status::SUCCESS, copy_dbm.open(copy_path, false))
        else
          params = {dbm: conf[:expected_class]}
          if open_params.has_key?(:num_shards)
            params[:num_shards] = 0
          end
          assert_equal(Status::SUCCESS, copy_dbm.open(copy_path, false, params))
        end
        assert_true(copy_dbm.healthy?)
        iter_records = {}
        copy_dbm.each do |key, value|
          iter_records[key] = value
        end
        assert_equal(records, iter_records)
        assert_equal(Status::SUCCESS, copy_dbm.close)
      end
      export_dbm = DBM.new
      assert_equal(Status::SUCCESS, export_dbm.open("", true, dbm: "BabyDBM"))
      assert_equal(Status::SUCCESS, dbm.export(export_dbm))
      iter_records = {}
      export_dbm.each do |key, value|
        iter_records[key] = value
      end
      assert_equal(records, iter_records)
      assert_equal(Status::SUCCESS, export_dbm.clear)
      assert_equal(0, export_dbm.to_i)
      assert_equal(Status::SUCCESS, export_dbm.set("1", "100"))
      value = export_dbm.increment("10000", 2, 10000, status)
      assert_equal(Status::SUCCESS, status)
      assert_equal(value, 10002)
      value = export_dbm.increment("10000", Utility::INT64MIN, 0, status)
      assert_equal(Status::SUCCESS, status)
      assert_equal(value, 10002)
      assert_equal(Status::DUPLICATION_ERROR, export_dbm.set("1", "101", false))
      assert_equal(Status::SUCCESS, export_dbm.compare_exchange("1", "100", "101"))
      value = export_dbm.increment("10000", 2)
      assert_equal(value, 10004)
      assert_equal(Status::INFEASIBLE_ERROR, export_dbm.compare_exchange("1", "100", "101"))
      assert_equal(Status::SUCCESS, export_dbm.compare_exchange("1", "101", nil))
      value = export_dbm.get("1", status)
      assert_equal(Status::NOT_FOUND_ERROR, status)
      assert_equal(Status::SUCCESS, export_dbm.compare_exchange("1", nil, "zzz"))
      assert_equal(Status::INFEASIBLE_ERROR, export_dbm.compare_exchange("1", nil, "yyy"))
      assert_equal("zzz", export_dbm.get("1", status))
      assert_equal(Status::SUCCESS, export_dbm.compare_exchange("1", "zzz", nil))
      iter = export_dbm.make_iterator
      assert_equal(Status::SUCCESS, iter.first)
      assert_equal(Status::SUCCESS, iter.set("foobar"))
      assert_equal(Status::SUCCESS, iter.remove)
      iter.destruct
      assert_equal(0, export_dbm.to_i)
      assert_equal(Status::SUCCESS, export_dbm.append("foo", "bar", ","))
      assert_equal(Status::SUCCESS, export_dbm.append("foo", "baz", ","))
      assert_equal(Status::SUCCESS, export_dbm.append("foo", "qux", ""))
      assert_equal("bar,bazqux", export_dbm.get("foo"))
      export_dbm["abc"] = "defg"
      assert_equal("defg", export_dbm["abc"])
      export_dbm.remove("abc")
      assert_equal(nil, export_dbm["abc"])
      assert_equal(Status::SUCCESS,
                   export_dbm.set_multi(one: "first", two: "second", three: "third"))
      ret_records = export_dbm.get_multi("one", "two")
      assert_equal("first", ret_records["one"])
      assert_equal("second", ret_records["two"])
      assert_equal(nil, ret_records["third"])
      assert_equal(Status::SUCCESS, export_dbm.remove_multi("one", "two"))
      assert_equal(Status::NOT_FOUND_ERROR, export_dbm.remove_multi("two", "three"))
      status = Status.new
      assert_equal(nil, export_dbm.get("one", status))
      assert_equal(Status::NOT_FOUND_ERROR, status)
      status = Status.new
      assert_equal(nil, export_dbm.get("two", status))
      assert_equal(Status::NOT_FOUND_ERROR, status)
      status = Status.new
      assert_equal(nil, export_dbm.get("three", status))
      assert_equal(Status::NOT_FOUND_ERROR, status)
      assert_equal(Status::SUCCESS, export_dbm.close)
      export_dbm.destruct
      assert_equal(Status::SUCCESS, dbm.close)
      dbm.destruct
    end      
  end

  # Iterator tests.
  def test_iterator
    confs = [
      {path: "casket.tkt",
       open_params: {num_buckets: 100, max_page_size: 50, max_branches: 2}},
      {path: "casket.tks",
       open_params: {step_unit: 3, max_level: 3}},
      {path: "casket.tkt",
       open_params: {num_shards: 4, num_buckets: 100, max_page_size: 50, max_branches: 2}},
    ]
    confs.each do |conf|
      path = conf[:path]
      path = path.empty? ? path : _make_tmp_path(path)
      dbm = DBM.new
      open_params = conf[:open_params].clone
      open_params[:truncate] = true
      open_params[:encoding] = "UTF-8"
      assert_equal(Status::SUCCESS, dbm.open(path, true, open_params))
      iter = dbm.make_iterator
      assert_equal(Status::SUCCESS, iter.first)
      assert_equal(Status::SUCCESS, iter.last)
      assert_equal(Status::SUCCESS, iter.jump(""))
      assert_equal(Status::SUCCESS, iter.jump_lower("", true))
      assert_equal(Status::SUCCESS, iter.jump_upper("", true))
      (1..100).each do |i|
        key = "%03d" % i
        value = "%d" % (i * i)
        assert_equal(Status::SUCCESS, dbm.set(key, value, false))
      end
      assert_equal(Status::SUCCESS, dbm.synchronize(false))
      assert_equal(Status::SUCCESS, iter.first())
      assert_equal("001", iter.get_key())
      assert_equal("1", iter.get_value())
      assert_equal(Status::SUCCESS, iter.last())
      assert_equal("100", iter.get_key())
      assert_equal("10000", iter.get_value())
      assert_equal(Status::SUCCESS, iter.jump("050"))
      assert_equal("050", iter.get_key())
      assert_equal(Status::SUCCESS, iter.jump_lower("050", true))
      assert_equal("050", iter.get_key())
      assert_equal(Status::SUCCESS, iter.previous())
      assert_equal("049", iter.get_key())
      assert_equal(Status::SUCCESS, iter.jump_lower("050", false))
      assert_equal("049", iter.get_key())
      assert_equal(Status::SUCCESS, iter.next())
      assert_equal("050", iter.get_key())
      assert_equal(Status::SUCCESS, iter.jump_upper("050", true))
      assert_equal("050", iter.get_key())
      assert_equal(Status::SUCCESS, iter.previous())
      assert_equal("049", iter.get_key())
      assert_equal(Status::SUCCESS, iter.jump_upper("050", false))
      assert_equal("051", iter.get_key())
      assert_equal(Status::SUCCESS, iter.next())
      assert_equal("052", iter.get_key())
      iter.destruct
      assert_equal(Status::SUCCESS, dbm.close)
      dbm.destruct
    end      
  end

  # Thread tests
  def test_thread
    dbm = DBM.new
    assert_equal(Status::SUCCESS, dbm.open(
                   "casket.tkh", true, truncate: true, num_buckets: 1000))
    rnd_state = Random.new
    num_records = 5000
    num_threads = 5
    records = {}
    tasks = []
    (0...num_threads).each do |param_thid|
      th = Thread.new(param_thid) do |thid|
        (0...num_records).each do |i|
          key_num = rnd_state.rand(num_records)
          key_num = key_num - key_num % num_threads + thid
          key = key_num.to_s
          value = (key_num * key_num).to_s
          if rnd_state.rand(num_records) == 0
            assert_equal(Status::SUCCESS, dbm.rebuild)
          elsif rnd_state.rand(10) == 0
            iter = dbm.make_iterator
            iter.jump(key)
            status = Status.new
            record = iter.get(status)
            if status == Status::SUCCESS
              assert_equal(2, record.size)
              assert_equal(key, record[0])
              assert_equal(value, record[1])
              iter.next.or_die
            end
          elsif rnd_state.rand(4) == 0
            status = Status.new
            rec_value = dbm.get(key, status)
            if status == Status::SUCCESS
              assert_equal(value, rec_value)
            else
              assert_equal(Status::NOT_FOUND_ERROR, status)
            end
          elsif rnd_state.rand(4) == 0
            status = dbm.remove(key)
            if status == Status::SUCCESS
              records.delete(key)
            else
              assert_equal(Status::NOT_FOUND_ERROR, status)
            end
          else
            overwrite = rnd_state.rand(2) == 0
            status = dbm.set(key, value, overwrite)
            if status == Status::SUCCESS
              records[key] = value
            else
              assert_equal(Status::DUPLICATION_ERROR, status)
            end
          end
          if rnd_state.rand(10) == 0
            Thread.pass
          end
        end
      end
      tasks.push(th)
    end
    tasks.each do |th|
      th.join
    end
    iter_records = {}
    dbm.each do |key, value|
      iter_records[key] = value
    end
    assert_equal(records, iter_records)
    assert_equal(Status::SUCCESS, dbm.close)
    dbm.destruct
  end

  # Search tests.
  def test_search
    confs = [
      {path: "casket.tkh",
       open_params: {num_buckets: 100}},
      {path: "casket.tkt",
       open_params: {num_buckets: 100}},
      {path: "casket.tks",
       open_params: {max_level: 8}},
      {path: "",
       open_params: {dbm: "TinyDBM", num_buckets: 100}},
      {path: "",
       open_params: {dbm: "BabyDBM"}},
    ]
    confs.each do |conf|
      path = conf[:path]
      path = path.empty? ? path : _make_tmp_path(path)
      dbm = DBM.new
      open_params = conf[:open_params].clone
      open_params[:truncate] = true
      assert_equal(Status::SUCCESS, dbm.open(path, true, open_params))
      (1..100).each do |i|
        key = "%08d" % i
        value = "%d" % i
        assert_equal(Status::SUCCESS, dbm.set(key, value, false))
      end
      assert_equal(Status::SUCCESS, dbm.synchronize(false))
      assert_equal(100, dbm.count)
      assert_equal(12, dbm.search("contain", "001").size)
      assert_equal(3, dbm.search("contain", "001", 3).size)
      assert_equal(10, dbm.search("begin", "0000001").size)
      assert_equal(10, dbm.search("end", "1").size)
      assert_equal(10, dbm.search("regex", "^\\d+1$").size)
      assert_equal(10, dbm.search("regex", "^\\d+1$", 0, true).size)
      assert_equal(3, dbm.search("edit", "00000100", 3, true).size)
      assert_equal(3, dbm.search("edit", "00000100", 3, false).size)
      assert_equal(Status::SUCCESS, dbm.close)
      dbm.destruct
    end
  end

  # Text tests.
  def test_text
    dbm = DBM.new
    dest_path = _make_tmp_path("casket.txt")
    assert_equal(Status::SUCCESS, dbm.open("", true))
    (1..100).each do |i|
      key = "%08d" % i
      value = "%d" % i
      assert_equal(Status::SUCCESS, dbm.set(key, value, false))
    end
    assert_equal(Status::SUCCESS, dbm.export_keys_as_lines(dest_path))
    assert_equal(Status::SUCCESS, dbm.close())
    dbm.destruct
    textfile = TextFile.new
    assert_equal(Status::SUCCESS, textfile.open(dest_path))
    assert_true(textfile.inspect.include?("TextFile"))
    assert_true(textfile.to_s.include?("TextFile"))
    assert_equal(12, textfile.search("contain", "001").size)
    assert_equal(3, textfile.search("contain", "001", 3).size)
    assert_equal(10, textfile.search("begin", "0000001").size)
    assert_equal(10, textfile.search("end", "1").size)
    assert_equal(10, textfile.search("regex", "^\\d+1$").size)
    assert_equal(10, textfile.search("regex", "^\\d+1$", 0, true).size)
    assert_equal(3, textfile.search("edit", "00000100", 3, true).size)
    assert_equal(3, textfile.search("edit", "00000100", 3, false).size)
    assert_equal(Status::SUCCESS, textfile.close)
  end
end
