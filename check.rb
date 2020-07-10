#! /usr/bin/ruby

require 'rbconfig'

commands = [
  'test.rb -v',
  'perf.rb --path casket.tkh --params "num_buckets=100000" --iter 20000 --threads 5',
  'perf.rb --path casket.tkh --params "concurrent=true,num_buckets=100000" --iter 20000 --threads 5 --random',
  'perf.rb --path casket.tkt --params "key_comparator=decimal" --iter 20000 --threads 5',
  'perf.rb --path casket.tkt --params "concurrent=true,key_comparator=decimal" --iter 20000 --threads 5 --random',
  'perf.rb --path casket.tks --params "step_unit=3" --iter 20000 --threads 5',
  'perf.rb --path casket.tks --params "concurrent=true,step_unit=3" --iter 20000 --threads 5 --random',
  'perf.rb --params "dbm=tiny,num_buckets=100000" --iter 20000 --threads 5 --random',
  'perf.rb --params "dbm=baby,key_comparator=decimal" --iter 20000 --threads 5 --random',
  'perf.rb --params "dbm=stdhash,num_buckets=100000" --iter 20000 --threads 5 --random',
  'perf.rb --params "dbm=stdtree" --iter 20000 --threads 5 --random',
  'wicked.rb --path casket.tkh --params "num_buckets=100000" --iter 20000 --threads 5',
  'wicked.rb --path casket.tkt --params "key_comparator=decimal" --iter 20000 --threads 5',
  'wicked.rb --path casket.tks --params "step_unit=3" --iter 20000 --threads 5',
  'wicked.rb --params "dbm=tiny,num_buckets=100000" --iter 20000 --threads 5',
  'wicked.rb --params "dbm=baby,key_comparator=decimal" --iter 20000 --threads 5',
]

system("rm -rf casket*")
rubycmd = RbConfig::CONFIG["bindir"] + "/" + RbConfig::CONFIG['ruby_install_name']
cnt = 0
oknum = 0
commands.each do |command|
  cnt += 1
  printf("%03d/%03d: %s: ", cnt, commands.size, command)
  rv = system("#{rubycmd} -I. #{command}")
  if rv
    oknum += 1
    printf("ok\n")
  else
    printf("failed\n")
  end
end
system("rm -rf casket*")
if oknum == cnt
  printf("%d tests were all ok\n", cnt)
else
  printf("%d/%d tests failed\n", cnt - oknum, cnt)
end
