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
MONGODB_URI = "mongodb://localhost:#{MONGOD_PORT}/#{MONGO_DBNAME}"
ENV['MONGODB_URI'] = MONGODB_URI

RSpec::Core::RakeTask.new(:spec)

def path_file_to_s(*args)
  File.join(*(args[0..-2] << file_to_s(File.join(*args))))
end

task :default do
  puts <<-EOF
  MONGODB_URI='#{MONGODB_URI}'
  usage:
    rake fetch
    rake unarchive
    rake mongo:start
    rake load_tables
    rake merge_1 merge_n
    rake metrics:wc_all
    rake metrics:wc_core
    rake metrics:dump
    rake metrics:bson
    rake references
    rake indexes
  EOF
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

$CreateTables_sql = "musicbrainz-server/admin/sql/CreateTables.sql"
$CreateTables_sql = 'schema/CreateTables.sql'

file 'schema/create_tables.json' => [ $CreateTables_sql, 'lib/parslet_sql.rb' ] do |file|
  sql_text = IO.read($CreateTables_sql)
  m = CreateTablesParser.new.parse(sql_text)
  File.open(file.name, 'w') {|fio| fio.write(JSON.pretty_generate(m)) }
end

desc "load_tables"
task :load_tables => 'schema/create_tables.json' do
  table_names = Dir["data/fullexport/#{file_to_s(LATEST_FILE)}/mbdump/*"].collect{|file_name| File.basename(file_name) }
  sh "MONGODB_URI='#{MONGODB_URI}' time ./script/mbdump_to_mongo.rb #{table_names.join(' ')}"
end

desc "merge_enums" # running this shows that enums from the schema are not used
task :merge_enums => 'schema/create_tables.json' do
  sh "MONGODB_URI='#{MONGODB_URI}' time ./script/merge_enum_types.rb"
end

desc "merge_1"
task :merge_1 do
  [
      ['area.type', 'area_type._id'],
      ['area_alias.type', 'area_alias_type._id'],
      ['artist.type', 'artist_type._id'],
      ['artist.gender', 'gender._id'],
      ['artist_alias.type', 'artist_alias_type._id'],
      ['artist_credit_name.artist_credit', 'artist_credit._id'],
      ['label.type', 'label_type._id'],
      ['label_alias.type', 'label_alias_type._id'],
      ['medium.format', 'medium_format._id'],
      ['medium_cdtoc.cdtoc', 'cdtoc._id'],
      ['place.type', 'place_type._id'],
      ['place_alias.type', 'place_alias_type._id'],
      ['release.language', 'language._id'],
      ['release.packaging', 'release_packaging._id'],
      ['release.script', 'script._id'],
      ['release.status', 'release_status._id'],
      ['release_country.country', 'country_area.area'],
      ['release_group.type', 'release_group_primary_type._id'],
      ['release_group_secondary_type_join.secondary_type', 'release_group_secondary_type._id'],
      ['script_language.language', 'language._id'],
      ['script_language.script', 'script._id'],
      ['track.medium', 'medium._id'],
      ['work.language', 'language._id'],
      ['work.type', 'work_type._id'],
      ['work_alias.type', 'work_alias_type._id'],
      ['work_attribute_type_allowed_value.work_attribute_type', 'work_attribute_type._id'], # must be before next
      ['work_attribute.work_attribute_type', 'work_attribute_type._id'],
      ['work_attribute.work_attribute_type_allowed_value', 'work_attribute_type_allowed_value._id'],
      # url by gid
      ['area.url', 'url.gid'],
      ['artist.url', 'url.gid'],
      ['label.url', 'url.gid'],
      ['place.url', 'url.gid'],
      ['recording.url', 'url.gid'],
      ['release.url', 'url.gid'],
      ['release_group.url', 'url.gid'],
      ['track.url', 'url.gid'],
      ['work.url', 'url.gid'],
      # core
      ['artist.area', 'area._id'],
      ['label.area', 'area._id'],
      ['release_label.label', 'label._id']
  ].each do |parent, child|
    sh "MONGODB_URI='#{MONGODB_URI}' time ./script/merge_1.rb #{parent} #{child} || true"
  end
end

desc "merge_n"
task :merge_n do
  [
      ['area.alias', 'area_alias.area'],
      ['area.iso_3166_1', 'iso_3166_1.area'],
      ['area.iso_3166_2', 'iso_3166_2.area'],
      ['area.iso_3166_3', 'iso_3166_3.area'],
      ['artist.alias', 'artist_alias.artist'],
      ['artist.artist_credit_name', 'artist_credit_name.artist'],
      ['artist.ipi', 'artist_ipi.artist'],
      ['artist.isni', 'artist_isni.artist'],
      ['label.alias', 'label_alias.label'],
      ['label.ipi', 'label_ipi.label'],
      ['label.isni', 'label_isni.label'],
      ['medium.cdtoc', 'medium_cdtoc.medium'],
      ['recording.isrc', 'isrc.recording'],
      ['recording.track', 'track.recording'],
      ['place.alias', 'place_alias.place'],
      ['release.country', 'release_country.release'],
      ['release.label', 'release_label.release'],
      ['release.unknown_country', 'release_unknown_country.release'],
      ['release_group.secondary_type', 'release_group_secondary_type_join.release_group'],
      ['work.alias', 'work_alias.work'],
      ['work.attribute', 'work_attribute.work'],
      ['work.iswc', 'iswc.work'],
      # gid_redirect by new_id
      ['area.gid_redirect', 'area_gid_redirect.new_id'],
      ['artist.gid_redirect', 'artist_gid_redirect.new_id'],
      ['label.gid_redirect', 'label_gid_redirect.new_id'],
      ['place.gid_redirect', 'place_gid_redirect.new_id'],
      ['recording.gid_redirect', 'recording_gid_redirect.new_id'],
      ['release.gid_redirect', 'release_gid_redirect.new_id'],
      ['release_group.gid_redirect', 'release_group_gid_redirect.new_id'],
      ['track.gid_redirect', 'track_gid_redirect.new_id'],
      ['url.gid_redirect', 'url_gid_redirect.new_id'],
      ['work.gid_redirect', 'work_gid_redirect.new_id'],
      # core
      ['area.place', 'place.area'],
      ['release_group.release', 'release.release_group']
  ].each do |parent, child|
    sh "MONGODB_URI='#{MONGODB_URI}' time ./script/merge_n.rb #{parent} #{child} || true"
  end
end

CORE_ENTITIES = %w(area artist label place recording release release_group url work)

namespace :metrics do
  task :wc_all do
    sh "cd #{DATA_LATEST_DIR}/mbdump && wc -l * | sort -nr"
  end
  task :wc_core do
    sh "cd #{DATA_LATEST_DIR}/mbdump && wc -l #{CORE_ENTITIES.join(' ')} | sort -nr"
  end
  task :dump do
    CORE_ENTITIES.each do |entity|
      sh "mongodump --port #{MONGOD_PORT} -d #{MONGO_DBNAME} -c #{entity}"
    end
  end
  task :bson do
    CORE_ENTITIES.each do |entity|
      #sh "script/bson_metrics.rb dump/#{MONGO_DBNAME}/#{entity}.bson"
      sh "../libbson/bson-metrics dump/#{MONGO_DBNAME}/#{entity}.bson"
    end
  end
end

# PK - Primary Key index hint
# references table.column - relation in comment

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

desc "print indexes from schema - does not ensure indexes yet"
task :indexes => 'schema/create_tables.json' do
  client = Mongo::MongoClient.from_uri(MONGODB_URI)
  db = client[MONGO_DBNAME]
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
  client.close
end
