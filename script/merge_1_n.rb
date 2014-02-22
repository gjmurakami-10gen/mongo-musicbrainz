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

USAGE = "usage: #{$0} parent.foreign_key child.id"
abort(USAGE) if ARGV.size != 2
parent_arg = ARGV[0].split('.', -1)
child_arg = ARGV[1].split('.', -1)
abort(USAGE) if parent_arg.size != 2 || child_arg.size != 2

parent_name, parent_key = parent_arg
parent_coll = $db[parent_name]
puts "#{parent_name} count: #{parent_coll.count}"

child_name, child_key = child_arg
child_coll = $db[child_name]
puts "#{child_name} count: #{child_coll.count}"

def hash_by_key(a, key)
  Hash[*a.collect{|e| [e[key], e]}.flatten(1)]
end

def ordered_group_by_first(pairs)
  pairs.inject([[], nil]) do |memo, pair|
    result, previous_value = memo
    current_value = pair.first
    result << [current_value, []] if previous_value != current_value
    result.last.last << pair.last
    [result, current_value]
  end.first
end

SLICE_SIZE = 5000

parent_coll.find.each_slice(SLICE_SIZE) do |doc_slice|
  bulk = parent_coll.initialize_unordered_bulk_op
  count = 0
  ids = doc_slice.collect{|doc| doc['_id']}
  many_docs = child_coll.find({child_key => {'$in' => ids}}).to_a
  next if many_docs.empty?
  many_docs_by_key = many_docs.collect{|doc| [doc[child_key], doc]}
  many_docs_by_key.sort!{|a,b| a.first <=> b.first}
  #puts "#{child_name} slice doc count:#{many_docs_by_key.count}"
  groups = ordered_group_by_first(many_docs_by_key)
  #puts "#{child_name} slice group count:#{groups.count}"
  print groups.count
  hash_doc_slice = hash_by_key(doc_slice, '_id')
  groups.each do |group|
    key = group.first
    doc = hash_doc_slice[key]
    doc[parent_key] = group.last
    bulk.find({'_id' => key}).replace_one(doc)
    count += 1
  end
  bulk.execute if count > 0
  putc('.')
end
puts
