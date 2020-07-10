#! /usr/bin/ruby

system("rm -rf api-doc")

File::open("tkrzw-doc.rb") { |ifile|
  File::open("tkrzw.rb", "w") { |ofile|
    ifile.each { |line|
      line = line.chomp
      line = line.sub(/# +@param +(\w+) +/, '# - <b>@param <i>\\1</i></b> ')
      line = line.sub(/# +@(\w+) +/, '# - <b>@\\1</b> ')
      ofile.printf("%s\n", line)
    }
  }
}

system('rdoc --title "Tkrzw" --main tkrzw.rb -o api-doc tkrzw.rb')
system("rm -f tkrzw.rb")
