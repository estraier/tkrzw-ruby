#! /usr/bin/ruby -I. -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Example for basic usage of the tree database
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

# END OF FILE
