#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for process methods
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

# END OF FILE
