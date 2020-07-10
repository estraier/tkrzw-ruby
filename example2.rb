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
    raise Tkrzw::StatusException.new(status)
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
