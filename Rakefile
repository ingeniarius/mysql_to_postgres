require 'rake'
require 'rake/testtask'
require 'rake/rdoctask'

desc 'Default: run unit tests.'
task :default => :test

desc 'Test the mysql_to_postgres plugin.'
Rake::TestTask.new(:test) do |t|
  t.libs << 'lib'
  t.pattern = 'test/**/*_test.rb'
  t.verbose = true
end


begin
  require 'jeweler'
  Jeweler::Tasks.new do |gemspec|
    gemspec.name = 'mysql_to_postgres'
    gemspec.summary = 'Rails rake task to migrate existing MySQL database schema ' +
                      'and content to Postgresql'
    gemspec.description = ''
    gemspec.email = 'jamestyj@gmail.com'
    gemspec.homepage = 'http://github.com/jamestyj/mysql_to_postgres'
    gemspec.authors = [ 'James Tan' ]
  end
  Jeweler::GemcutterTasks.new
rescue LoadError
  puts 'Jeweler (or a dependency) not available. ' +
       'Install it with: sudo gem install jeweler.'
end

