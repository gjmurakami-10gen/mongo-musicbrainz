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

$enum_types = enum_types($create_tables)
$enum_types.each do |type_name, enum|
  p([type_name, enum])

end



