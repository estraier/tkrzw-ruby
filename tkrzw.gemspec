require 'rubygems'

spec = Gem::Specification.new do |s|
  s.name = "Tkrzw"
  s.version = "0.1.3"
  s.author = "Mikio Hirabayashi"
  s.email = "hirarin@gmail.com"
  s.homepage = "http://dbmx.net/tkrzw/"
  s.summary = "Tkrzw: a set of implementations of DBM"
  s.description = "DBM (Database Manager) is a concept of libraries to store an associative array on a permanent storage."
  s.files = [ "tkrzw.cc", "extconf.rb" ]
  s.require_path = "."
  s.extensions = [ "extconf.rb" ]
end

if $0 == __FILE__
  Gem::manage_gems
  Gem::Builder.new(spec).build
end
