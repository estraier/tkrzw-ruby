#! /usr/bin/ruby -w
# -*- coding: utf-8 -*-
#--------------------------------------------------------------------------------------------------
# Building configurations
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

require "mkmf"

File::unlink("Makefile") if File::exist?("Makefile")
dir_config('tkrzw')

home = ENV["HOME"]
ENV["PATH"] = ENV["PATH"] + ":/usr/local/bin:$home/bin:."
tkcflags = `tkrzw_build_util config -i 2>/dev/null`.chomp
tkldflags = `tkrzw_build_util config -l 2>/dev/null`.chomp
tkldflags = tkldflags.gsub(/-l[\S]+/, "").strip
tklibs = `tkrzw_build_util config -l 2>/dev/null`.chomp
tklibs = tklibs.gsub(/-L[\S]+/, "").strip

tkcflags = "-I/usr/local/include" if tkcflags.length < 1
tkldflags = "-L/usr/local/lib" if tkldflags.length < 1
tklibs = "-ltkrzw -lstdc++ -lrt -latomic -lpthread -lm -lc" if tklibs.length < 1

RbConfig::CONFIG["CPP"] = "g++ -std=c++17 -E"
$CFLAGS = "-std=c++17 -Wno-register -I. #{tkcflags} -Wall #{$CFLAGS} -O2"
$CXXFLAGS = "-std=c++17 -Wno-register -I. #{tkcflags} -Wall #{$CFLAGS} -O2"
$LDFLAGS = "#{$LDFLAGS} -L. #{tkldflags}"
$libs = "#{$libs} #{tklibs}"

printf("setting variables ...\n")
printf("  \$CFLAGS = %s\n", $CFLAGS)
printf("  \$LDFLAGS = %s\n", $LDFLAGS)
printf("  \$libs = %s\n", $libs)

if have_header('tkrzw_lib_common.h')
  create_makefile('tkrzw')
end

# END OF FILE
