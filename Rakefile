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
  File.exists?(file) ? IO.read(file).chomp : "NIL"
end

FTP_FULLEXPORT_DIR = "ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
FTP_FULLEXPORT_DIR_ABSOLUTE = File.absolute_path(FTP_FULLEXPORT_DIR)
LATEST_FILE = "#{FTP_FULLEXPORT_DIR}/LATEST"
CURRENT_FILE = "CURRENT"
LATEST = file_to_s(LATEST_FILE)
DB_TIME_ID = ENV['DB_TIME_ID'] || file_to_s(CURRENT_FILE) || LATEST
FTP_LATEST_DIR = "#{FTP_FULLEXPORT_DIR}/#{LATEST}"
DATA_LATEST_DIR = "data/fullexport/#{LATEST}"

MONGO_DBPATH = "data/db/#{DB_TIME_ID}"
MONGOD_PORT = 37017
MONGOD_LOCKPATH = "#{MONGO_DBPATH}/mongod.lock"
MONGOD_LOGPATH = "#{MONGO_DBPATH}/mongod.log"
MONGO_DBNAME = "musicbrainz"
MONGODB_URI = "mongodb://localhost:#{MONGOD_PORT}/#{MONGO_DBNAME}"
ENV['MONGODB_URI'] = MONGODB_URI

MERGE_SPEC = 'schema/merge_spec_flat.json'

RSpec::Core::RakeTask.new(:spec)

def path_file_to_s(*args)
  File.join(*(args[0..-2] << file_to_s(File.join(*args))))
end

ORDERED_TASKS = %w[
    latest
    fetch
    unarchive
    cutover
    metrics:wc_all
    metrics:wc_core
    references
    mongo:start
    mongo:status
    spec
    load_tables
    metrics:mongo
    indexes
    merge:all
    metrics:mongo
    metrics:dump
    metrics:bson
    mongo:stop
]

task :default do
  puts <<-EOF
  MONGODB_URI='#{MONGODB_URI}'
  usage - initial:
    rake -T # print rake tasks
    rake all # all of the following
  usage - individual:
    #{ORDERED_TASKS.collect{|task| "rake #{task}"}.join("\n    ")}
  EOF
end

task :all do
  log_file_name = 'rake_all.log'
  sh "> #{log_file_name}"
  puts "# run the following in another window for progress"
  puts "tail -f #{log_file_name}"
  ORDERED_TASKS.each do |task|
    sh "(time rake #{task}) >> #{log_file_name} 2>&1"
  end
end

file LATEST_FILE do |file|
  sh "wget --recursive --progress=dot:giga ftp://#{file.name}" # need --recursive to retrieve the new version
end

desc "latest"
task :latest do
  sh "mv #{LATEST_FILE} #{LATEST_FILE}.#{LATEST} || true"
  Rake::Task[LATEST_FILE].execute
end

desc "fetch"
task :fetch => LATEST_FILE do
  sh "wget --recursive --level=1 --continue #{FTP_LATEST_DIR}"
end

desc "cutover"
task :cutover => 'mongo:stop' do
  sh "cp #{LATEST_FILE} #{CURRENT_FILE}"
end

desc "unarchive"
task :unarchive => LATEST_FILE do
  mbdump_tar = File.join(File.absolute_path(FTP_LATEST_DIR), 'mbdump.tar.bz2')
  FileUtils.mkdir_p(DATA_LATEST_DIR)
  Dir.chdir(DATA_LATEST_DIR)
  sh "tar -xf '#{mbdump_tar}'"
end

namespace :mongo do
  task :start do
    FileUtils.mkdir_p(MONGO_DBPATH) unless File.directory?(MONGO_DBPATH)
    sh "mongod --dbpath #{MONGO_DBPATH} --port #{MONGOD_PORT} --fork --logpath #{MONGOD_LOGPATH}"
  end
  task :status do
    sh "ps -fp #{file_to_s(MONGOD_LOCKPATH)} || true" if File.size?(MONGOD_LOCKPATH)
  end
  task :stop do
    sh "kill #{file_to_s(MONGOD_LOCKPATH)}" if File.size?(MONGOD_LOCKPATH)
  end
  task :shell do
    sh "mongo --port #{MONGOD_PORT} '#{MONGO_DBNAME}'"
  end
end

RSpec::Core::RakeTask.new(:spec)

CORE_ENTITIES = %w(area artist label place recording release release_group url work)

# https://github.com/metabrainz/musicbrainz-server
# git clone --recursive https://github.com/metabrainz/musicbrainz-server.git
$CreateTables_sql = "../musicbrainz-server/admin/sql/CreateTables.sql"
$CreateTables_sql = 'schema/CreateTables.sql' # override - no sub-project yet for musicbrainz-server

# PK - Primary Key index hint
# references table.column - relation in comment

file 'schema/create_tables.json' => [ $CreateTables_sql, 'lib/parslet_sql.rb' ] do |file|
  sql_text = IO.read($CreateTables_sql)
  m = CreateTablesParser.new.parse(sql_text)
  File.open(file.name, 'w') {|fio| fio.write(JSON.pretty_generate(m)) }
end

desc "print references from schema"
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

desc "load_tables"
task :load_tables => 'schema/create_tables.json' do
  table_names = Dir["data/fullexport/#{DB_TIME_ID}/mbdump/*"].collect{|file_name| File.basename(file_name) }
  sh "MONGODB_URI='#{MONGODB_URI}' time ./script/mbdump_to_mongo.rb #{table_names.join(' ')}"
end

desc "print indexes from schema - does not ensure indexes yet"
task :indexes => 'schema/create_tables.json' do
  #client = Mongo::MongoClient.from_uri(MONGODB_URI)
  #db = client[MONGO_DBNAME]
  JSON.parse(IO.read('schema/create_tables.json')).each do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      table_name = create_table['table_name']
      columns = create_table['columns']
      columns.each do |column|
        column_name = column['column_name']
        comment = column['comment']
        if comment =~ /PK/
          #puts "table_name:#{table_name} column_name:#{column_name} comment:#{comment.inspect}"
          column_name = '_id' if column_name == 'id'
          puts "#{table_name}.#{column_name} PK"
          #collection = db[table_name]
          #collection.ensure_index(column_name => Mongo::ASCENDING)
        end
      end
    end
  end
  #client.close
end

desc "merge_enums" # running this shows that enums from the schema are not used
task :merge_enums => 'schema/create_tables.json' do
  sh "MONGODB_URI='#{MONGODB_URI}' time ./script/merge_enum_types.rb"
end

task :merge_data_check do
  JSON.parse(IO.read(MERGE_SPEC)).each do |spec|
    x, parent_spec, child_spec = spec
    parent_collection, parent_key = parent_spec.split('.', 2)
    child_collection, child_key = child_spec.split('.', 2)
    composite_name = "#{parent_collection}_#{parent_key}"
    if x == '1'
      puts "warning: spec:#{spec.inspect} child_key:#{child_key.inspect} is not \"_id\"" if child_key != "_id"
      if parent_key != child_collection && composite_name != child_collection
        puts "warning: spec:#{spec.inspect} child_collection:#{child_collection.inspect} is not parent_key:#{parent_key.inspect}"
      end
    else
      puts "warning: spec:#{spec.inspect} child_key:#{child_key.inspect} is not parent_collection:#{parent_collection.inspect}" if child_key != parent_collection
      if parent_key != child_collection && composite_name != child_collection
        puts "warning: spec:#{spec.inspect} parent_key:#{parent_key.inspect} is not child_collection:#{child_collection.inspect}"
      end
      puts "warning: spec:#{spec.inspect} parent_key:#{parent_key.inspect} includes parent_collection:#{parent_collection.inspect}" if parent_key.include? parent_collection
    end
  end
end

def group_by_first(pairs)
  pairs.inject([[], nil]) do |memo, pair|
    result, previous_value = memo
    current_value = pair.first
    if previous_value != current_value
      result << [current_value, []]
    end
    result.last.last << pair.last
    [result, current_value]
  end.first
end

task :merge_spec_group do
  merge_spec_flat = JSON.parse(IO.read(MERGE_SPEC))
  merge_spec_with_group_key = merge_spec_flat.collect do |x, parent, child|
    parent_collection = parent.split('.', 2).first
    [parent_collection, [x, parent, child]]
  end
  merge_spec_group = group_by_first(merge_spec_with_group_key)
  merge_spec = merge_spec_group.collect do |parent_collection, spec|
    [
        parent_collection,
        spec.collect do |x, parent, child|
          parent_name, parent_key = parent.split('.', 2)
          child_name, child_key = child.split('.', 2)
          if x == '1'
            child_key = nil if child_key == '_id' # child_key default is '_id'
            child_name = '' if child_name == parent_key # child_name defaut is parent_key
            child_spec = [child_name, child_key].compact.join('.')
            child_spec = nil if child_spec.empty?
            [parent_key, child_spec].compact.join(':')
          elsif x == 'n'
            child_key = nil if child_key == parent_name # child_key default is parent_name
            child_name = '' if child_name == parent_key # child_name defaut is parent_key
            child_spec = [child_name, child_key].compact.join('.')
            child_spec = nil if child_spec.empty?
            [parent_key, "[#{child_spec}]"].compact.join(':')
          else
            raise "not reached"
          end
        end
    ]
  end
  pp merge_spec #IO.write('spec/merge_spec_group.json', PP.pp(merge_spec, ""))
end

namespace :merge do
  spec_group = JSON.parse(IO.read('spec/merge_spec_group.json'))
  spec_group.each do |parent_collection, children|
    dependencies = children.collect do |child|
      if (match_data = /^(?<parent_key>[^:]+)(:\[?(?<child_collection>[^.\]]*))?/.match(child))
        parent_key = match_data[:parent_key]
        child_collection = match_data[:child_collection] || parent_key
        child_collection = parent_key if child_collection.empty?
        #puts "child:#{child.inspect} child_collection:#{child_collection.inspect}"
        child_collection.to_sym
      else
        raise "unrecognized merge spec:#{child.inspect}"
      end
    end
    task parent_collection.to_sym => dependencies do
      sh "(time script/merge.rb #{parent_collection} #{children.join(' ')}) #> log/merge_#{parent_collection} 2>&1"
    end
  end
  task :all => spec_group.collect{|spec|spec.first}
  rule /.*/ do |task|
    #puts "rule: #{task.name}"
  end
end

desc "merge"
task :merge do
  JSON.parse(IO.read(MERGE_SPEC)).each do |x, parent, child|
    sh "MONGODB_URI='#{MONGODB_URI}' time ./script/merge_#{x}.rb #{parent} #{child} || true"
  end
end

namespace :metrics do
  task :wc_all do
    sh "cd #{DATA_LATEST_DIR}/mbdump && wc -l * | sort -nr"
  end
  task :wc_core do
    sh "cd #{DATA_LATEST_DIR}/mbdump && wc -l #{CORE_ENTITIES.join(' ')} | sort -nr"
  end
  task :mongo do
    client = Mongo::MongoClient.from_uri(MONGODB_URI)
    db = client[MONGO_DBNAME]
    collection_names = (db.collection_names - ["system.indexes"]).sort
    coll_stats = collection_names.collect{|collection_name| db.command({collStats: collection_name})}
    puts JSON.pretty_generate(coll_stats)
  end
  task :dump do
    CORE_ENTITIES.each do |entity|
      sh "mongodump --port #{MONGOD_PORT} -d #{MONGO_DBNAME} -c #{entity}"
    end
  end
  task :bson do
    paths = CORE_ENTITIES.collect{|entity| "dump/#{MONGO_DBNAME}/#{entity}.bson"}
    CORE_ENTITIES.each do |entity|
      #sh "script/bson_metrics.rb #{paths.join(' ')}"
      sh "../libbson/bson-metrics #{paths.join(' ')}"
    end
  end
end

task :clobber do
  if ENV['FORCE'] == 'REALLY_FORCE'
    Rake::Task['mongo:stop'].execute
    sh "rm -fr data dump log/* rake_all.log"
  else
    puts "usage: rake FORCE=REALLY_FORCE clobber"
  end
end
