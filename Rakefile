# Copyright (C) 2009-2014 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

require 'fileutils'
require 'json'
require 'pp'
require 'rspec/core/rake_task'
require_relative 'lib/parslet_sql'
require 'mongo'

# http://www.postgresql.org/docs/9.1/static/sql-createtable.html

def file_to_s(file)
  IO.read(file).chomp
end

FTP_FULLEXPORT_DIR = File.absolute_path("ftp.musicbrainz.org/pub/musicbrainz/data/fullexport")
LATEST_FILE = "#{FTP_FULLEXPORT_DIR}/LATEST"
LATEST = file_to_s(LATEST_FILE)
FTP_LATEST_DIR = "#{FTP_FULLEXPORT_DIR}/#{LATEST}"
DATA_LATEST_DIR = "data/fullexport/#{LATEST}"

MONGO_DBPATH = "data/db/#{LATEST}"
MONGOD_PORT = 37017
MONGOD_LOCKPATH = "#{MONGO_DBPATH}/mongod.lock"
MONGOD_LOGPATH = "#{MONGO_DBPATH}/mongod.log"
MONGO_DBNAME = "musicbrainz"
MONGODB_URI = "mongodb://localhost:#{MONGOD_PORT}"
ENV['MONGODB_URI'] = MONGODB_URI

RSpec::Core::RakeTask.new(:spec)

def path_file_to_s(*args)
  File.join(*(args[0..-2] << file_to_s(File.join(*args))))
end

task :default => [:load_tables] do
  sh "echo Hello World!"
end

file LATEST_FILE do |file|
  sh "wget --recursive ftp://#{file.name}" # need --recursive to retrieve the new version
end

desc "fetch"
task :fetch => LATEST_FILE do
  sh "wget --recursive -level=1 --continue #{FTP_LATEST_DIR}"
end

desc "unarchive"
task :unarchive => LATEST_FILE do
  mbdump_tar = File.join(FTP_LATEST_DIR, 'mbdump.tar.bz2')
  FileUtils.mkdir_p(DATA_LATEST_DIR)
  Dir.chdir(DATA_LATEST_DIR)
  sh "tar -xf '#{mbdump_tar}'"
end

$CreateTables_sql = "musicbrainz-server/admin/sql/CreateTables.sql"
$CreateTables_sql = 'schema/CreateTables.sql'

file 'schema/create_tables.json' => [ $CreateTables_sql, 'lib/parslet_sql.rb' ] do |file|
  sql_text = IO.read($CreateTables_sql)
  m = CreateTablesParser.new.parse(sql_text)
  File.open(file.name, 'w') {|fio| fio.write(JSON.pretty_generate(m)) }
end

namespace :mongo do
  task :start do
    FileUtils.mkdir_p(MONGO_DBPATH) unless File.directory?(MONGO_DBPATH)
    sh "mongod --dbpath #{MONGO_DBPATH} --port #{MONGOD_PORT} --fork --logpath #{MONGOD_LOGPATH}"
  end
  task :status do
    sh "ps -fp #{file_to_s(MONGOD_LOCKPATH)}" if File.size?(MONGOD_LOCKPATH)
  end
  task :stop do
    sh "kill #{file_to_s(MONGOD_LOCKPATH)}" if File.size?(MONGOD_LOCKPATH)
  end
end

desc "load_tables"
task :load_tables => 'schema/create_tables.json' do
  table_names = Dir["data/fullexport/#{file_to_s(LATEST_FILE)}/mbdump/*"].collect{|file_name| File.basename(file_name) }
  sh "time ./script/mbdump_to_mongo.rb #{table_names.join(' ')}"
end

desc "merge_enums"
task :merge_enums => 'schema/create_tables.json' do  table_names = Dir["data/fullexport/#{file_to_s(LATEST_FILE)}/mbdump/*"].collect{|file_name| File.basename(file_name) }
  sh "time ./script/merge_enum_types.rb"
end

# PK - Primary Key index hint
# references table.column - relation in comment

task :indexes => 'schema/create_tables.json' do
  client = Mongo::MongoClient.from_uri(MONGODB_URI)
  db = $client[MONGO_DBNAME]
  JSON.parse(IO.read('schema/create_tables.json')).each do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      table_name = create_table['table_name']
      columns = create_table['columns']
      columns.each do |column|
        column_name = column['column_name']
        comment = column['comment']
        if comment =~ /PK/
          puts "table_name:#{table_name} column_name:#{column_name} comment:#{comment.inspect}"
          #collection = db[table_name]
          #collection.ensure_index(column_name => Mongo::ASCENDING)
        end
      end
    end
  end
  client.close
end

task :references => 'schema/create_tables.json' do
  JSON.parse(IO.read('schema/create_tables.json')).each do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      table_name = create_table['table_name']
      columns = create_table['columns']
      columns.each do |column|
        column_name = column['column_name']
        comment = column['comment']
        if comment =~ /references/
          reference = comment[/references\s+([.\w]+)/,1] #comment[/references\s+([\w]+\.g?id)/,1]
          raise "#{table_name}.#{column_name} #{comment}" if !reference && comment !~ /language|weakly|attribute_type|country_area/
          puts "#{table_name}.#{column_name} references #{reference}"
        end
      end
    end
  end
end
