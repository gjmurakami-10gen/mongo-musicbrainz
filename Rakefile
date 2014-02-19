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

# http://www.postgresql.org/docs/9.1/static/sql-createtable.html

FTP_BASE = "ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
LATEST = "#{FTP_BASE}/LATEST"
MBDUMP = "#{FTP_BASE}/#{IO.read(LATEST).chomp}/mbdump.tar.bz2"

RSpec::Core::RakeTask.new(:spec)

def file_to_s(file)
  IO.read(file).chomp
end

def path_file_to_s(*args)
  File.join(*(args[0..-2] << file_to_s(File.join(*args))))
end

task :default => [:load_tables] do
  sh "echo Hello World!"
end

file LATEST do |file|
  sh "wget --recursive ftp://#{file.name}" # need --recursive to retrieve the new version
end

task :fetch => LATEST do
  sh "wget --recursive -level=1 --continue #{path_file_to_s(FTP_BASE, 'LATEST')}"
end

task :unarchive => LATEST do
  mbdump_tar = File.join(path_file_to_s(File.dirname(__FILE__), FTP_BASE, 'LATEST'), 'mbdump.tar.bz2')
  dest_dir = "data/fullexport/#{file_to_s(LATEST)}"
  FileUtils.mkdir_p(dest_dir)
  Dir.chdir(dest_dir)
  sh "tar -xf '#{mbdump_tar}'"
end

$CreateTables_sql = "musicbrainz-server/admin/sql/CreateTables.sql"
$CreateTables_sql = 'schema/CreateTables.sql'

file 'schema/create_tables.json' => [ $CreateTables_sql, 'lib/parslet_sql.rb' ] do |file|
  sql_text = IO.read($CreateTables_sql)
  m = CreateTablesParser.new.parse(sql_text)
  File.open(file.name, 'w') {|fio| fio.write(JSON.pretty_generate(m)) }
end

# PK - Primary Key index hint
# references table.column - relation in comment

task :references => 'schema/create_tables.json' do
  JSON.parse(IO.read('schema/create_tables.json')).each do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      columns = create_table['columns']
      columns.each do |column|
        comment = column['comment']
        pp comment if comment =~ /references/
      end
    end
  end
end

task :extract => 'schema/create_tables.json' do
  JSON.parse(IO.read('schema/create_tables.json')).each do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      table_name = create_table['table_name']
      p table_name
      sh "tar -tf #{MBDUMP} mbdump/#{table_name}"
    end
  end
end

task :load_tables => 'schema/create_tables.json' do
  table_names = Dir["data/fullexport/#{file_to_s(LATEST)}/mbdump/*"].collect{|file_name| File.basename(file_name) }
  p table_names
  sh "./script/mbdump_to_mongo.rb #{table_names.join(' ')}"
end
