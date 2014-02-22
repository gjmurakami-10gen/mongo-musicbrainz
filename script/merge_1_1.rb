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

def hash_by_key(a, key)
  Hash[*a.collect{|e| [e[key], e]}.flatten(1)]
end

BASE_DIR = File.expand_path('../..', __FILE__)
FULLEXPORT_DIR = "#{BASE_DIR}/ftp.musicbrainz.org/pub/musicbrainz/data/fullexport"
LATEST = "#{FULLEXPORT_DIR}/LATEST"
SCHEMA_FILE = "#{BASE_DIR}/schema/create_tables.json"
MONGO_DBNAME = "musicbrainz"

$create_tables = JSON.parse(IO.read(SCHEMA_FILE))
$client = Mongo::MongoClient.from_uri
$db = $client[MONGO_DBNAME]
$collection = nil

USAGE = "usage: #{$0} parent.foreign_key child.id"
abort(USAGE) if ARGV.size != 2
parent_arg = ARGV[0].split('.', -1)
child_arg = ARGV[1].split('.', -1)
abort(USAGE) if parent_arg.size != 2 || child_arg.size != 2

coll_name, coll_key = child_arg
coll = $db[coll_name]
docs = coll.find.to_a
puts "#{coll_name} count: #{docs.count}"
child_hash = hash_by_key(docs, coll_key)
#p child_hash

SLICE_SIZE = 1000

coll_name, coll_key = parent_arg
coll = $db[coll_name]
puts "#{coll_name} count: #{coll.count}"
coll.find.each_slice(SLICE_SIZE) do |doc_slice|
  bulk = coll.initialize_unordered_bulk_op
  count = 0
  doc_slice.each do |doc|
    fk = doc[coll_key]
    next unless fk
    child_doc = child_hash[fk]
    abort("exit: #{$0} #{ARGV.join(' ')} - already applied - fk:#{fk.inspect}") unless child_doc
    #puts("warning: #{$0} #{ARGV.join(' ')} - already applied - fk:#{fk.inspect}") unless child_doc
    next unless child_doc
    doc[coll_key] = child_doc if child_doc
    bulk.find({'_id' => doc['_id']}).replace_one(doc)
    count += 1
  end
  bulk.execute if count > 0
end