#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for secondary index
#
# Copyright 2020 Google LLC
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
# except in compliance with the License.  You may obtain a copy of the License at
#     https://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software distributed under the
# License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied.  See the License for the specific language governing permissions
# and limitations under the License.
#--------------------------------------------------------------------------------------------------

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

# END OF FILE
