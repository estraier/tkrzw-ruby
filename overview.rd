= Ruby Binding of Tkrzw

Tkrzw: a set of implementations of DBM

== Introduction

DBM (Database Manager) is a concept to store an associative array on a permanent storage.  In other words, DBM allows an application program to store key-value pairs in a file and reuse them later.  Each of keys and values is a string or a sequence of bytes.  A key must be unique within the database and a value is associated to it.  You can retrieve a stored record with its key very quickly. Thanks to simple structure of DBM, its performance can be extremely high.

Tkrzw is a library implementing DBM with various algorithms.  It features high degrees of performance, concurrency, scalability and durability.  The following data structures are provided.

- HashDBM : File datatabase manager implementation based on hash table.
- TreeDBM : File datatabase manager implementation based on B+ tree.
- SkipDBM : File datatabase manager implementation based on skip list.
- TinyDBM : On-memory datatabase manager implementation based on hash table.
- BabyDBM : On-memory datatabase manager implementation based on B+ tree.
- CacheDBM : On-memory datatabase manager implementation with LRU deletion.
- StdHashDBM : On-memory DBM implementations using std::unordered_map.
- StdTreeDBM : On-memory DBM implementations using std::map.

Whereas Tkrzw is C++ library, this package provides its Ruby interface.  All above data structures are available via one adapter class "DBM". Read the {homepage}[https://dbmx.net/tkrzw/] for details.

The following classes are mainly used.

- Tkrzw::Utility : Library utilities.
- Tkrzw::Status : Status of operations
- Tkrzw::DBM : Polymorphic database manager
- Tkrzw::Iterator : Iterator for each record
- Tkrzw::Future : Future containing a status object and extra data
- Tkrzw::AsyncDBM : Asynchronous database manager adapter
- Tkrzw::File : Generic file implementation
- Tkrzw::Index : Secondary index
- Tkrzw::IndexIterator : Iterator for each record of the secondary index

DBM stores key-value pairs of strings.  You can specify any type of objects as keys and values if they can be converted into strings.  When you retreive keys and values, they are always represented as strings.

The module file is "tkrzw", which defines the module "Tkrzw".

  require 'tkrzw'

An instance of the class "DBM" is used in order to handle a database.  You can store, delete, and retrieve records with the instance.  The result status of each operation is represented by an object of the class "Status".  Iterator to access access each record is implemented by the class "Iterator".

== Installation

Install the latest version of Tkrzw beforehand and get the package of the Python binding of Tkrzw.  Ruby 2.5 or later is required to use this package.

Enter the directory of the extracted package then perform installation.

  ruby extconf.rb
  make
  ruby check.rb
  sudo make install

== Example

The following code is a typical example to use a database.  A DBM object can be used like a Hash object.  The "each" iterator is useful to access each record in the database.

  require 'tkrzw'
  
  # Prepares the database.
  dbm = Tkrzw::DBM.new
  dbm.open("casket.tkh", true, truncate: true,num_buckets: 100)
  
  # Sets records.
  dbm["first"] = "hop"
  dbm["second"] = "step"
  dbm["third"] = "jump"
  
  # Retrieves record values.
  # If the operation fails, nil is returned.
  p dbm["first"]
  p dbm["second"]
  p dbm["third"]
  p dbm["fourth"]
  
  # Checks and deletes a record.
  if dbm.include?("first")
    dbm.remove("first")
  end
  
  # Traverses records.
  dbm.each do |key, value|
    p key + ": " + value
  end
   
  # Closes and the database.
  dbm.close

The following code is a more complex example.  You should use "ensure" clauses to destruct instances of DBM and Iterator, in order to release unused resources.  Even if the database is not closed, the destructor closes it implicitly.  The method "or_die" throws an exception on failure so it is useful for checking errors.

  require 'tkrzw'

  dbm = Tkrzw::DBM.new
  begin
    # Prepares the database.
    # Options are given as a Hash object.
    open_params = {
      max_page_size: 4080,
      max_branches: 256,
      key_comparator: "decimal",
      concurrent: true,
      encoding: "UTF-8",
      truncate: true,
    }
    status = dbm.open("casket.tkt", true, open_params)
    if not status.ok?
      raise Tkrzw::StatusExceptio.new(status)
    end
  
    # Sets records.
    # The method OrDie raises a runtime error on failure.
    dbm.set(1, "hop").or_die
    dbm.set(2, "step").or_die
    dbm.set(3, "jump").or_die
   
    # Retrieves records without checking errors.
    p dbm.get(1)
    p dbm.get(2)
    p dbm.get(3)
    p dbm.get(4)
   
    # To know the status of retrieval, give a status object to "get".
    # You can compare a status object and a status code directly.
    status = Tkrzw::Status.new
    value = dbm.get(1, status)
    printf("status: %s\n", status)
    if status == Tkrzw::Status::SUCCESS
      printf("value: %s\n", value)
    end
   
    # Rebuilds the database.
    # Almost the same options as the "open" method can be given.
    dbm.rebuild(align_pow: 0, max_page_size: 1024).or_die
   
    # Traverses records with an iterator.
    begin
      iter = dbm.make_iterator
      iter.first
      while true do
        status = Tkrzw::Status.new
        record = iter.get(status)
        break if not status.ok?
        printf("%s: %s\n", record[0], record[1])
        iter.next
      end
    ensure
      # Releases the resources.
      iter.destruct
    end
  
    # Closes the database.
    dbm.close.or_die
  ensure
    # Releases the resources.
    dbm.destruct
  end

The following code is a typical example of the asynchronous API.  The AsyncDBM class manages a thread pool and handles database operations in the background in parallel.  Each Method of AsyncDBM returns a Future object to monitor the result.

  require 'tkrzw'
  
  dbm = Tkrzw::DBM.new
  dbm.open("casket.tkh", true, truncate: true, num_buckets: 100)
  
  # Prepares the asynchronous adapter with 4 worker threads.
  async = Tkrzw::AsyncDBM.new(dbm, 4)
  
  # Executes the Set method asynchronously.
  future = async.set("hello", "world")
  # Does something in the foreground.
  until future.wait(0)
    puts("Setting a record")
  end
  # Checks the result after awaiting the Set operation.
  status = future.get
  if status != Tkrzw::Status::SUCCESS
    puts("ERROR: " + status.to_s)
  end
  
  # Executes the Get method asynchronously.
  future = async.get("hello")
  # Does something in the foreground.
  puts("Getting a record")
  # Awaits the operation and get the result.
  status, value = future.get()
  if status == Tkrzw::Status::SUCCESS
    puts("VALUE: " + value)
  end
  
  # Releases the asynchronous adapter.
  async.destruct
  
  # Closes the database.
  dbm.close

The following code uses process, process_multi, and process_each functions which take blocks to process the record efficiently.  process is useful to update a record atomically according to the current value.  process_each is useful to access every record in the most efficient way.

  require 'tkrzw'
  
  # Opens the database.
  dbm = Tkrzw::DBM.new
  dbm.open("casket.tkh", true, truncate: true, num_buckets: 1000)
  
  # Sets records with blocks.
  dbm.process("doc-1", true) {|key, value| "Tokyo is the capital city of Japan."}
  dbm.process("doc-2", true) {|key, value| "Is she living in Tokyo, Japan?"}
  dbm.process("doc-3", true) {|key, value| "She must leave Tokyo!"}
  
  # Lowers record values.
  def lower(key, value)
    # If no matching record, nil is given as the value.
    return nil if not value
    # Sets the new value.
    return value.downcase
  end
  dbm.process("doc-1", true) {|k, v| lower(k, v)}
  dbm.process("doc-2", true) {|k, v| lower(k, v)}
  dbm.process("doc-3", true) {|k, v| lower(k, v)}
  dbm.process("non-existent", true){|k, v| lower(k, v)}
  
  # If you don't update the record, set the second parameter to false.
  dbm.process("doc-3", false) {|k, v| p k, v}
  
  # Adds multiple records at once.
  records = {"doc-4"=>"Tokyo Go!", "doc-5"=>"Japan Go!"}
  dbm.process_multi(["doc-4", "doc-5"], true) {|k, v|
    records[k]
  }
  
  # Modifies multiple records at once.
  dbm.process_multi(["doc-4", "doc-5"], true) {|k, v| lower(k, v)}
  
  # Checks the whole content.
  # This uses an external iterator and is relavively slow.
  dbm.each do |key, value|
    p key + ": " + value
  end
  
  # Function for word counting.
  def word_counter(key, value, counts)
    return if not key
    value.split(/\W+/).each {|word|
      counts[word] = (counts[word] or 0) + 1
    }
  end
  word_counts = {}
  
  # The parameter should be false if the value is not updated.
  dbm.process_each(false) {|k, v| word_counter(k, v, word_counts)}
  p word_counts
  
  # Returning False by the callbacks removes the record.
  dbm.process("doc-1", true) {|k, v| false}
  p dbm.count
  dbm.process_multi(["doc-2", "doc-3"], true) {|k, v| false}
  p dbm.count
  dbm.process_each(true) {|k, v| false}
  p dbm.count
  
  # Closes the database.
  dbm.close

The following code is an example to use a secondary index, which is useful to organize records by non-primary keys.

  require 'tkrzw'
  
  # Opens the incex.
  index = Tkrzw::Index.new
  index.open("casket.tkt", true, truncate: true, num_buckets: 100).or_die
  
  # Adds records to the index.
  # The key is a division name and the value is person name.
  index.add("general", "anne").or_die
  index.add("general", "matthew").or_die
  index.add("general", "marilla").or_die
  index.add("sales", "gilbert").or_die
  
  # Anne moves to the sales division.
  index.remove("general", "anne").or_die
  index.add("sales", "anne").or_die
  
  # Prints all members for each division.
  ["general", "sales"].each do |division|
    puts(division)
    members = index.get_values(division)
    members.each do |member|
      puts(" -- " + member)
    end
  end
  
  # Prints every record by iterator.
  iter = index.make_iterator
  iter.first
  loop do
    record = iter.get
    break if not record
    puts(record[0] + ": " + record[1])
    iter.next
  end
  iter.destruct
  
  # Prints every record by the iterator block
  index.each do |key, value|
    puts(key + ": " + value)
  end
  
  # Closes the index.
  index.close.or_die
