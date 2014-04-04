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
    merge
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

MERGE_SPEC = [
    ["n", "area.alias", "area_alias.area"],
    ["n", "area.gid_redirect", "area_gid_redirect.new_id"],
    ["n", "area.iso_3166_1", "iso_3166_1.area"],
    ["n", "area.iso_3166_2", "iso_3166_2.area"],
    ["n", "area.iso_3166_3", "iso_3166_3.area"],
    ["n", "area.place", "place.area"],
    ["1", "area.type", "area_type._id"],
    ["1", "area_alias.type", "area_alias_type._id"],
    ["n", "artist.alias", "artist_alias.artist"],
    ["1", "artist.area", "area._id"],
    ["1", "artist.gender", "gender._id"],
    ["n", "artist.gid_redirect", "artist_gid_redirect.new_id"],
    ["n", "artist.ipi", "artist_ipi.artist"],
    ["n", "artist.isni", "artist_isni.artist"],
    ["n", "artist.name", "artist_credit_name.artist"],
    ["1", "artist.type", "artist_type._id"],
    ["1", "artist_alias.type", "artist_alias_type._id"],
    ["1", "artist_credit_name.artist_credit", "artist_credit._id"],
    ["1", "country_area.area", "area._id"],
    ["n", "label.alias", "label_alias.label"],
    ["1", "label.area", "area._id"],
    ["n", "label.gid_redirect", "label_gid_redirect.new_id"],
    ["n", "label.ipi", "label_ipi.label"],
    ["n", "label.isni", "label_isni.label"],
    ["1", "label.type", "label_type._id"],
    ["1", "label_alias.type", "label_alias_type._id"],
    ["n", "medium.cdtoc", "medium_cdtoc.medium"],
    ["1", "medium.format", "medium_format._id"],
    ["n", "medium.track", "track.medium"],
    ["1", "medium_cdtoc.cdtoc", "cdtoc._id"],
    ["n", "place.alias", "place_alias.place"],
    ["n", "place.gid_redirect", "place_gid_redirect.new_id"],
    ["1", "place.type", "place_type._id"],
    ["1", "place_alias.type", "place_alias_type._id"],
    ["n", "recording.gid_redirect", "recording_gid_redirect.new_id"],
    ["n", "recording.isrc", "isrc.recording"],
    ["n", "recording.track", "track.recording"],
    ["n", "release.country", "release_country.release"],
    ["n", "release.gid_redirect", "release_gid_redirect.new_id"],
    ["n", "release.label", "release_label.release"],
    ["1", "release.language", "language._id"],
    ["n", "release.medium", "medium.release"],
    ["1", "release.packaging", "release_packaging._id"],
    ["1", "release.script", "script._id"],
    ["1", "release.status", "release_status._id"],
    ["n", "release.unknown_country", "release_unknown_country.release"],
    ["1", "release_country.country", "country_area._id"],
    ["n", "release_group.gid_redirect", "release_group_gid_redirect.new_id"],
    ["n", "release_group.release", "release.release_group"],
    ["n", "release_group.secondary_type", "release_group_secondary_type_join.release_group"],
    ["1", "release_group.type", "release_group_primary_type._id"],
    ["1", "release_group_secondary_type_join.secondary_type", "release_group_secondary_type._id"],
    ["1", "release_label.label", "label._id"],
    ["1", "script_language.language", "language._id"],
    ["1", "script_language.script", "script._id"],
    ["n", "track.gid_redirect", "track_gid_redirect.new_id"],
    ["n", "url.gid_redirect", "url_gid_redirect.new_id"],
    ["n", "work.alias", "work_alias.work"],
    ["n", "work.attribute", "work_attribute.work"],
    ["n", "work.gid_redirect", "work_gid_redirect.new_id"],
    ["n", "work.iswc", "iswc.work"],
    ["1", "work.language", "language._id"],
    ["1", "work.type", "work_type._id"],
    ["1", "work_alias.type", "work_alias_type._id"],
    ["1", "work_attribute.work_attribute_type", "work_attribute_type._id"],
    ["1", "work_attribute.work_attribute_type_allowed_value", "work_attribute_type_allowed_value._id"],
    ["1", "work_attribute_type_allowed_value.work_attribute_type", "work_attribute_type._id"]
]

task :merge_data_check do
  MERGE_SPEC.each do |spec|
    x, parent_spec, child_spec = spec
    parent_collection, parent_key = parent_spec.split('.', -1)
    child_collection, child_key = child_spec.split('.', -1)
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

desc "merge"
task :merge do
  MERGE_SPEC.each do |x, parent, child|
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
    sh "rm -fr data dump rake_all.log"
  else
    puts "usage: rake FORCE=REALLY_FORCE clobber"
  end
end

