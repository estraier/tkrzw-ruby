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
 
# Traverses records.
dbm.each do |key, value|
  p key + ": " + value
end
 
# Closes and the database and releases the resources.
dbm.close
dbm.destruct
