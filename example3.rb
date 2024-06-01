#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for key comparators of the tree database
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

# Opens a new database with the default key comparator (LexicalKeyComparator).
dbm = Tkrzw::DBM.new
open_params = {
  truncate: true,
}
status = dbm.open("casket.tkt", true, open_params).or_die

# Sets records with the key being a big-endian binary of an integer.
# e.g: "\x00\x00\x00\x00\x00\x00\x00\x31" -> "hop"
dbm.set(Tkrzw::Utility::serialize_int(1), "hop").or_die
dbm.set(Tkrzw::Utility::serialize_int(256), "step").or_die
dbm.set(Tkrzw::Utility::serialize_int(32), "jump").or_die

# Gets records with the key being a big-endian binary of an integer.
p dbm.get(Tkrzw::Utility::serialize_int(1))
p dbm.get(Tkrzw::Utility::serialize_int(256))
p dbm.get(Tkrzw::Utility::serialize_int(32))

# Lists up all records, restoring keys into integers.
iter = dbm.make_iterator
iter.first
while true do
  record = iter.get(status)
  break if not record
  printf("%d: %s\n", Tkrzw::Utility::deserialize_int(record[0]), record[1])
  iter.next
end
iter.destruct

# Closes the database.
dbm.close.or_die
dbm.destruct

# Opens a new database with the decimal integer comparator.
dbm = Tkrzw::DBM.new
open_params = {
  truncate: true,
  key_comparator: "Decimal",
}
status = dbm.open("casket.tkt", true, open_params).or_die

# Sets records with the key being a decimal string of an integer.
# e.g: "1" -> "hop"
dbm.set("1", "hop").or_die
dbm.set("256", "step").or_die
dbm.set("32", "jump").or_die

# Gets records with the key being a decimal string of an integer.
p dbm.get("1")
p dbm.get("256")
p dbm.get("32")

# Lists up all records, restoring keys into integers.
iter = dbm.make_iterator
iter.first
while true do
  record = iter.get(status)
  break if not record
  printf("%d: %s\n", record[0].to_i, record[1])
  iter.next
end
iter.destruct

# Closes the database.
dbm.close.or_die
dbm.destruct

# Opens a new database with the decimal real number comparator.
dbm = Tkrzw::DBM.new
open_params = {
  truncate: true,
  key_comparator: "RealNumber",
}
status = dbm.open("casket.tkt", true, open_params).or_die

# Sets records with the key being a decimal string of a real number.
# e.g: "1.5" -> "hop"
dbm.set("1.5", "hop").or_die
dbm.set("256.5", "step").or_die
dbm.set("32.5", "jump").or_die

# Gets records with the key being a decimal string of a real number.
p dbm.get("1.5")
p dbm.get("256.5")
p dbm.get("32.5")

# Lists up all records, restoring keys into floating-point numbers.
iter = dbm.make_iterator
iter.first
while true do
  record = iter.get(status)
  break if not record
  printf("%.3f: %s\n", record[0].to_f, record[1])
  iter.next
end
iter.destruct

# Closes the database.
dbm.close.or_die
dbm.destruct

# Opens a new database with the big-endian floating-point numbers comparator.
dbm = Tkrzw::DBM.new
open_params = {
  truncate: true,
  key_comparator: "FloatBigEndian",
}
status = dbm.open("casket.tkt", true, open_params).or_die

# Sets records with the key being a big-endian binary of a floating-point number.
# e.g: "\x3F\xF8\x00\x00\x00\x00\x00\x00" -> "hop"
dbm.set(Tkrzw::Utility::serialize_float(1.5), "hop").or_die
dbm.set(Tkrzw::Utility::serialize_float(256.5), "step").or_die
dbm.set(Tkrzw::Utility::serialize_float(32.5), "jump").or_die

# Gets records with the key being a decimal string of an integer.
p dbm.get(Tkrzw::Utility::serialize_int(1))
p dbm.get(Tkrzw::Utility::serialize_int(256))
p dbm.get(Tkrzw::Utility::serialize_int(32))

# Lists up all records, restoring keys into integers.
iter = dbm.make_iterator
iter.first
while true do
  record = iter.get(status)
  break if not record
  printf("%d: %s\n", Tkrzw::Utility::deserialize_int(record[0]), record[1])
  iter.next
end
iter.destruct

# Closes the database.
dbm.close.or_die
dbm.destruct

# END OF FILE
