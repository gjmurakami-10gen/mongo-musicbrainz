#!/usr/bin/env ruby
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

require 'time'
require 'json'
require 'pp'
require 'mongo'
require 'benchmark'
require 'ruby-prof'
require 'trollop'

BASE_DIR = File.expand_path('../..', __FILE__)
FULLEXPORT_DIR = "#{BASE_DIR}/ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
LATEST = "#{FULLEXPORT_DIR}/LATEST"
SCHEMA_FILE = "#{BASE_DIR}/schema/create_tables.json"
MONGO_DBNAME = "musicbrainz"

$create_tables = JSON.parse(IO.read(SCHEMA_FILE))
$client = Mongo::MongoClient.from_uri
$db = $client[MONGO_DBNAME]
$collection = nil

$options = {}

def enum_types(create_tables)
  create_types = create_tables.select{|sql| sql.has_key?('create_type') && sql['create_type'].has_key?('enum') }
  types_as_enum = create_types.collect{|sql| sql['create_type']}
  enum_types = types_as_enum.collect do |type_as_enum|
    [type_as_enum['type_name'], type_as_enum['enum'].collect{|enum| enum['string']}]
  end
  Hash[*enum_types.flatten(1)]
end

def type_columns(create_tables, type_name)
  table_columns = create_tables.collect do |sql|
    if sql.has_key?('create_table')
      create_table = sql['create_table']
      table_name = create_table['table_name']
      column_names = create_table['columns'].collect {|column|
        (column['data_type'] == type_name) ? column['column_name'] : nil
      }.compact
      column_names.empty? ? nil : [table_name, column_names]
    else
      nil
    end
  end
  table_columns.compact
end

$enum_types = enum_types($create_tables)
$enum_types.each do |type_name, enum|
  p([type_name, enum])
  type_columns($create_tables, type_name).each do |table_name, column_names|
    if $db.collection_names.include?(table_name)
      $collection = $db[table_name]
      column_names.each do |column_name|
        p column_name
        p $collection.find({column_name => {'$exists' => true}}).first
      end
    else
      puts "collection #{table_name} not found"
    end
  end
end



