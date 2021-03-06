#--
# Ruby binding of Tkrzw.
#++
#:include:overview.rd


# Namespace of Tkrzw.
module Tkrzw

  # Library utilities.
  class Utility
    # The package version numbers.
    VERSION = "0.0.0"
    # The minimum value of int32.
    INT32MIN = -2 ** 31
    # The maximum value of int32.
    INT32MAX = 2 ** 31 - 1
    # The maximum value of uint32.
    UINT32MAX = 2 ** 32 - 1
    # The minimum value of int64.
    INT64MIN = -2 ** 63
    # The maximum value of int64.
    INT64MAX = 2 ** 63 - 1
    # The maximum value of uint64.
    UINT64MAX = 2 ** 64 - 1
    # Gets the current memory usage of the process.
    def self.get_memory_usage()
      # (native code)
    end

    # Primary hash function for the hash database.
    # @param data The data to calculate the hash value for.
    # @param num_buckets The number of buckets of the hash table.  If it is omitted, 1<<64 is set.
    # @return The hash value.
    def self.primary_hash(data, num_buckets=nil)
      # (native code)
    end

    # Secondary hash function for sharding.
    # @param data The data to calculate the hash value for.
    # @param num_shards The number of shards.  If it is omitted, 1<<64 is set.
    # @return The hash value.
    def self.secondary_hash(data, num_shards=nil)
      # (native code)
    end

    # Gets the Levenshtein edit distance of two strings.
    # @param a A string.
    # @param b The other string.
    # @param utf If true, strings are treated as UTF-8 and the edit distance is by the Unicode character.
    # @return The Levenshtein edit distance of the two strings.
    def self.edit_distance_lev(a, b, utf=false):
      # (native code)
    end
  end

  # Status of operations.
  class Status
    # Success.
    SUCCESS = 0
    # Generic error whose cause is unknown.
    UNKNOWN_ERROR = 1
    # Generic error from underlying systems.
    SYSTEM_ERROR = 2
    # Error that the feature is not implemented.
    NOT_IMPLEMENTED_ERROR = 3
    # Error that a precondition is not met.
    PRECONDITION_ERROR = 4
    # Error that a given argument is invalid.
    INVALID_ARGUMENT_ERROR = 5
    # Error that the operation is canceled.
    CANCELED_ERROR = 6
    # Error that a specific resource is not found.
    NOT_FOUND_ERROR = 7
    # Error that the operation is not permitted.
    PERMISSION_ERROR = 8
    # Error that the operation is infeasible.
    INFEASIBLE_ERROR = 9
    # Error that a specific resource is duplicated.
    DUPLICATION_ERROR = 10
    # Error that internal data are broken.
    BROKEN_DATA_ERROR = 11
    # Generic error caused by the application logic.
    APPLICATION_ERROR = 12

    # Sets the code and the message.
    # @param code The status code.  This can be omitted and then SUCCESS is set.
    # @param message An arbitrary status message.  This can be omitted and the an empty string is set.
    def initialize(code=SUCCESS, message="")
      # (native code)
    end

    # Sets the code and the message.
    # @param code The status code.  This can be omitted and then SUCCESS is set.
    # @param message An arbitrary status message.  This can be omitted and the an empty string is set.
    def set(code=SUCCESS, message="")
      # (native code)
    end

    # Gets the status code.
    # @return The status code.
    def code()
      # (native code)
    end

    # Gets the status message.
    # @return The status message.
    def message()
      # (native code)
    end

    # Returns true if the status is success.
    # @return True if the status is success, or False on failure.
    def ok?()
      # (native code)
    end

    # Raises an exception if the status is not success.
    # @raise StatusException An exception containing the status object.
    def or_die()
      # (native code)
    end

    # Returns A string representation of the content.
    # @return The string representation of the content.
    def to_s()
      # (native code)
    end

    # Returns the status code.
    # @return The status code.
    def to_i()
      # (native code)
    end

    # Returns A string representation of the object.
    # @return The string representation of the object.
    def inspect()
      # (native code)
    end

    # Returns True if the other object has the same code.
    # @param rhs The object to compare.  It can be a status or an integer.
    # @return True if they are the same, or False if they are not.
    def ==(rhs)
      # (native code)
    end

    # Returns True if the other object doesn't have the same code.
    # @param rhs The object to compare.  It can be a status or an integer.
    # @return False if they are the same, or True if they are not.
    def !=(rhs)
      # (native code)
    end

  end

  # Exception to convey the status of operations.
  class StatusException < RuntimeError
    # Sets the status.
    # @param status The status object.
    def initialize(status)
      # (native code)
    end

    # Returns A string representation of the content.
    # @return The string representation of the content.
    def to_s()
      # (native code)
    end

    # Returns A string representation of the object.
    # @return The string representation of the object.
    def inspect()
      # (native code)
    end

    # Gets the status object
    # @return The status object.
    def status()
      # (native code)
    end
  end

  # Polymorphic database manager.
  # All operations except for "open" and "close" are thread-safe; Multiple threads can access the same database concurrently.  You can specify a data structure when you call the "open" method.  Every opened database must be closed explicitly by the "close" method to avoid data corruption.  Moreover, every unused database object should be destructed by the "destruct" method to free resources.
  class DBM

    # Does nothing especially.
    def initialize()
      # (native code)
    end

    # Opens a database file.
    # @param path A path of the file.
    # @param writable If true, the file is writable.  If false, it is read-only.
    # @param params Optional parameters of a hash object.
    # @return The result status.
    # The extension of the path indicates the type of the database.
    # - .tkh : File hash database (HashDBM)
    # - .tkt : File tree database (TreeDBM)
    # - .tks : File skip database (SkipDBM)
    # - .tkmt : On-memory hash database (TinyDBM)
    # - .tkmb : On-memory tree database (BabyDBM)
    # - .tkmc : On-memory cache database (CacheDBM)
    # - .tksh : On-memory STL hash database (StdHashDBM)
    # - .tkst : On-memory STL tree database (StdTreeDBM)
    # The optional parameters can include an option for the concurrency tuning.  By default, database operatins are done under the GVL (Global Virtual-machine Lock), which means that database operations are not done concurrently even if you use multiple threads.  If the "concurrent" parameter is true, database operations are done outside the GVL, which means that database operations can be done concurrently if you use multiple threads.  However, the downside is that swapping thread data is costly so the actual throughput is often worse in the concurrent mode than in the normal mode.  Therefore, the concurrent mode should be used only if the database is huge and it can cause blocking of threads in multi-thread usage.<br>
    # By default, the encoding of retrieved record data by the "get" method is implicitly set as "ASCII-8BIT".  If you want to change the implicit encoding to "UTF-8" or others, set the encoding name as the value of the "encoding" parameter.
    # The optional parameters can include options for the file opening operation.
    # - truncate (bool) True to truncate the file.
    # - no_create (bool) True to omit file creation.
    # - no_wait (bool) True to fail if the file is locked by another process.
    # - no_lock (bool) True to omit file locking.
    # The optional parameter "dbm" supercedes the decision of the database type by the extension.  The value is the type name: "HashDBM", "TreeDBM", "SkipDBM", "TinyDBM", "BabyDBM", "CacheDBM", "StdHashDBM", "StdTreeDBM".<br>
    # For HashDBM, these optional parameters are supported.
    # - update_mode (string) How to update the database file: "UPDATE_IN_PLACE" for the in-palce and "UPDATE_APPENDING" for the appending mode.
    # - offset_width (int) The width to represent the offset of records.
    # - align_pow (int) The power to align records.
    # - num_buckets (int) The number of buckets for hashing.
    # - fbp_capacity (int) The capacity of the free block pool.
    # - lock_mem_buckets (bool): True to lock the memory for the hash buckets.
    # For TreeDBM, all optional parameters for HashDBM are available.  In addition, these optional parameters are supported.
    # - max_page_size (int) The maximum size of a page.
    # - max_branches (int) The maximum number of branches each inner node can have.
    # - max_cached_pages (int) The maximum number of cached pages.
    # - key_comparator (string) The comparator of record keys: "LexicalKeyComparator" for the lexical order, "LexicalCaseKeyComparator" for the lexical order ignoring case, "DecimalKeyComparator" for the order of the decimal integer numeric expressions, "HexadecimalKeyComparato" for the order of the hexadecimal integer numeric expressions, "RealNumberKeyComparator" for the order of the decimal real number expressions.<br>
    # For SkipDBM, these optional parameters are supported.
    # - offset_width (int) The width to represent the offset of records.
    # - step_unit (int) The step unit of the skip list.
    # - max_level (int) The maximum level of the skip list.
    # - sort_mem_size (int) The memory size used for sorting to build the database in the at-random mode.
    # - insert_in_order (bool) If true, records are assumed to be inserted in ascending order of the key.
    # - max_cached_records (int) The maximum number of cached records.
    # For TinyDBM, these optional parameters are supported.
    # - num_buckets (int) The number of buckets for hashing.
    # For BabyDBM, these optional parameters are supported.
    # - key_comparator (string) The comparator of record keys. The same ones as TreeDBM.
    # For CacheDBM, these optional parameters are supported.
    # - cap_rec_num (int): The maximum number of records.
    # - cap_mem_size (int): The total memory size to use.
    # If the optional parameter "num_shards" is set, the database is sharded into multiple shard files.  Each file has a suffix like "-00003-of-00015".  If the value is 0, the number of shards is set by patterns of the existing files, or 1 if they doesn't exist.
    def open(path, writable, params = {})
      # (native code)
    end

    # Closes the database file.
    # @return The result status.
    def close()
      # (native code)
    end

    # Gets the value of a record of a key.
    # @param key The key of the record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The value of the matching record or nil on failure.
    def get(key, status=nil)
      # (native code)
    end

    # Gets the values of multiple records of keys.
    # @param keys The keys of records to retrieve.
    # @return A map of retrieved records.  Keys which don't match existing records are ignored.
    def get_multi(*keys)
      # (native code)
    end

    # Sets a record of a key and a value.
    # @param key The key of the record.
    # @param value The value of the record.
    # @param overwrite Whether to overwrite the existing value.  It can be omitted and then false is set.
    # @return The result status.
    def set(key, value, overwrite=false)
      # (native code)
    end

    # Sets multiple records of the keyword arguments.
    # @param records Records to store.  Existing records with the same keys are overwritten.
    # @return The result status.
    def set_multi(records = {})
      # (native code)
    end
  
    # Removes a record of a key.
    # @param key The key of the record.
    # @return The result status.
    def remove(key)
      # (native code)
    end

    # Appends data at the end of a record of a key.
    # @param key The key of the record.
    # @param value The value to append.
    # @param delim The delimiter to put after the existing record.
    # @return The result status.
    # If there's no existing record, the value is set without the delimiter.
    def append(key, value, delim="")
      # (native code)
    end

    # Compares the value of a record and exchanges if the condition meets.
    # @param key The key of the record.
    # @param expected The expected value.
    # @param desired The desired value.  If it is nil, the record is to be removed.
    # @return The result status.
    # If the record doesn't exist, NOT_FOUND_ERROR is returned.  If the existing value is different from the expected value, DUPLICATION_ERROR is returned.  Otherwise, the desired value is set.
    def compare_exchange(key, expected, desired)
      # (native code)
    end

    # Increments the numeric value of a record.
    # @param key The key of the record.
    # @param inc The incremental value.  If it is Utility::INT64MIN, the current value is not changed and a new record is not created.
    # @param init The initial value.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The current value, or nil on failure.
    # The record value is stored as an 8-byte big-endian integer.  Negative is also supported.
    def increment(key, inc=1, init=0, status=nil)
      # (native code)
    end

    # Gets the number of records.
    # @return The number of records on success, or nil on failure.
    def count()
      # (native code)
    end
  
    # Gets the current file size of the database.
    # @return The current file size of the database, or nil on failure.
    def file_size()
      # (native code)
    end

    # Gets the path of the database file.
    # @return The file path of the database, or nil on failure.
    def file_path()
      # (native code)
    end

    # Removes all records.
    # @return The result status.
    def clear()
      # (native code)
    end

    # Rebuilds the entire database.
    # @param params Optional parameters of a hash object.
    # @return The result status.
    # The optional parameters are the same as the "open" method.  Omitted tuning parameters are kept the same or implicitly optimized.
    def rebuild(params={})
      # (native code)
    end

    # Checks whether the database should be rebuilt.
    # @return True to be optimized or False with no necessity.
    def should_be_rebuilt()
      # (native code)
    end

    # Synchronizes the content of the database to the file system.
    # @param hard True to do physical synchronization with the hardware or false to do only logical synchronization with the file system.
    # @param params Optional parameters of a hash object.
    # Only SkipDBM uses the optional parameters.  The "merge" parameter specifies paths of databases to merge, separated by colon.  The "reducer" parameter specifies the reducer to apply to records of the same key.  "ReduceToFirst", "ReduceToSecond", "ReduceToLast", etc are supported.
    def synchronize(hard, params={})
      # (native code)
    end

    # Copies the content of the database file to another file.
    # @param dest_path A path to the destination file.
    # @return The result status.
    def copy_file(dest_path)
      # (native code)
    end
  
    # Exports all records to another database.
    # @param dest_dbm The destination database.
    # @return The result status.
    def export(dest_dbm)
      # (native code)
    end

    # Exports the keys of all records as lines to a text file.
    # @param dest_path A path of the output text file.
    # @return The result status.
    def export_keys_as_lines(dest_path)
      # (native code)
    end

    # Inspects the database.
    # @return A hash of property names and their values.
    def inspect_details()
      # (native code)
    end

    # Checks whether the database is open.
    # @return True if the database is open, or false if not.
    def open?()
      # (native code)
    end

    # Checks whether the database condition is healthy.
    # @return True if the database condition is healthy, or false if not.
    def healthy?()
      # (native code)
    end

    # Checks whether ordered operations are supported.
    # @return True if ordered operations are supported, or false if not.
    def ordered?()
      # (native code)
    end

    # Searches the database and get keys which match a pattern.
    # @param mode The search mode.  "contain" extracts keys containing the pattern.  "begin" extracts keys beginning with the pattern.  "end" extracts keys ending with the pattern.  "regex" extracts keys partially matches the pattern of a regular expression.  "edit" extracts keys whose edit distance to the pattern is the least.
    # @param pattern The pattern for matching.
    # @param capacity The maximum records to obtain.  0 means unlimited.
    # @param utf If true, text is treated as UTF-8, which affects "regex" and "edit".
    # @return A list of keys matching the condition.
    def search(mode, pattern, capacity=0, utf=false)
      # (native code)
    end
  
    # Makes an iterator for each record.
    # @return The iterator for each record.
    def make_iterator()
      # (native code)
    end

    # Returns A string representation of the content.
    # @return The string representation of the content.
    def to_s()
      # (native code)
    end

    # Gets the number of records, to enable the len operator.
    # @return The number of records on success, or -1 on failure.
    def to_i()
      # (native code)
    end

    # Returns A string representation of the object.
    # @return The string representation of the object.
    def inspect()
      # (native code)
    end

    # Gets the value of a record, to enable the [] operator.
    # @param key The key of the record.
    # @return The value of the matching record or nil on failure.
    def [](key)
      # (native code)
    end

    # Sets a record of a key and a value, to enable the []= operator.
    # @param key The key of the record.
    # @param value The value of the record.
    # @return The value of the matching record or nil on failure.
    def []=(key, value)
      # (native code)
    end

    # Calls the given block with the key and the value of each record
    def each(&block)
      # (native code)
    end
  end

  # Iterator for each record.
  # An iterator is made by the "make_iterator" method of DBM.  Every unused iterator object should be destructed explicitly by the "destruct" method to free resources.
  class Iterator

    # Initializes the iterator.
    # @param dbm The database to scan.
    def initialize(dbm)
      # (native code)
    end

    # Initializes the iterator to indicate the first record.
    # @return The result status.
    # Even if there's no record, the operation doesn't fail.
    def first()
      # (native code)
    end
  
    # Initializes the iterator to indicate the last record.
    # @return The result status.
    # Even if there's no record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def last()
      # (native code)
    end

    # Initializes the iterator to indicate a specific record.
    # @param key The key of the record to look for.
    # @return The result status.
    # Ordered databases can support "lower bound" jump; If there's no record with the same key, the iterator refers to the first record whose key is greater than the given key.  The operation fails with unordered databases if there's no record with the same key.
    def jump(key)
      # (native code)
    end

    # Initializes the iterator to indicate the last record whose key is lower than a given key.
    # @param key The key to compare with.
    # @param inclusive If true, the considtion is inclusive: equal to or lower than the key.
    # @return The result status.
    # Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def jump_lower(key, inclusive=false)
      # (native code)
    end

    # Initializes the iterator to indicate the first record whose key is upper than a given key.
    # @param key The key to compare with.
    # @param inclusive If true, the considtion is inclusive: equal to or upper than the key.
    # @return The result status.
    # Even if there's no matching record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def jump_upper(key, inclusive=false)
      # (native code)
    end
  
    # Moves the iterator to the next record.
    # @return The result status.
    # If the current record is missing, the operation fails.  Even if there's no next record, the operation doesn't fail.
    def next()
      # (native code)
    end

    # Moves the iterator to the previous record.
    # @return The result status.
    # If the current record is missing, the operation fails.  Even if there's no previous record, the operation doesn't fail.  This method is suppoerted only by ordered databases.
    def previous()
      # (native code)
    end

    # Gets the key and the value of the current record of the iterator.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return A tuple of The key and the value of the current record.  On failure, nil is returned.
    def get(status=nil)
      # (native code)
    end

    # Gets the key of the current record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The key of the current record or nil on failure.
    def get_key(status=nil)
      # (native code)
    end

    # Gets the value of the current record.
    # @param status A status object to which the result status is assigned.  It can be omitted.
    # @return The value of the current record or nil on failure.
    def get_value(status=nil)
      # (native code)
    end
  
    # Sets the value of the current record.
    # @param value The value of the record.
    # @return The result status.
    def set(value)
      # (native code)
    end

    # Removes the current record.
    # @return The result status.
    def remove()
      # (native code)
    end

    # Returns A string representation of the content.
    # @return The string representation of the content.
    def to_s()
      # (native code)
    end

    # Returns A string representation of the object.
    # @return The string representation of the object.
    def inspect()
      # (native code)
    end

  end

  # Text file of line data.
  # DBM#ExportKeysAsLines outputs keys of the database into a text file.  Scanning the text file is more efficient than scanning the whole database.
  class TextFile

    # Initializes the text file object.
    def initialize()
      # (native code)
    end

    # Opens a text file.
    # @param path A path of the file.
    # @return The result status.
    def open(path)
      # (native code)
    end

    # Closes the text file.
    # @return The result status.
    def close()
      # (native code)
    end

    # Searches the text file and get lines which match a pattern.
    # @param mode The search mode.  "contain" extracts lines containing the pattern.  "begin" extracts lines beginning with the pattern.  "end" extracts lines ending with the pattern.  "regex" extracts lines partially matches the pattern of a regular expression.  "edit" extracts lines whose edit distance to the pattern is the least.
    # @param pattern The pattern for matching.
    # @param capacity The maximum records to obtain.  0 means unlimited.
    # @param utf If true, text is treated as UTF-8, which affects "regex" and "edit".
    # @return A list of lines matching the condition.
    def search(mode, pattern, capacity=0, utf=false)
      # (native code)
    end

    # Returns A string representation of the content.
    # @return The string representation of the content.
    def to_s()
      # (native code)
    end

    # Returns A string representation of the object.
    # @return The string representation of the object.
    def inspect()
      # (native code)
    end
  end
end


# END OF FILE
