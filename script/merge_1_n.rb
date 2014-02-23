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

def ordered_group_by_first(pairs)
  pairs.inject([[], nil]) do |memo, pair|
    result, previous_value = memo
    current_value = pair.first
    result << [current_value, []] if previous_value != current_value
    result.last.last << pair.last
    [result, current_value]
  end.first
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

parent_name, parent_key = parent_arg
parent_coll = $db[parent_name]
puts "info: parent #{parent_name.inspect} count: #{parent_coll.count}"

child_name, child_key = child_arg
child_coll = $db[child_name]
child_count = child_coll.count
puts "info: child #{child_name.inspect} count: #{child_count}"

SLICE_SIZE = 5000
THRESHOLD = 10000
puts "info: ******** over #{THRESHOLD} threshold ********" if child_count > THRESHOLD

if child_count <= THRESHOLD
  child_docs = child_coll.find({child_key => {'$exists' => true}}).to_a
  puts "info: child #{child_name.inspect} find key #{child_key.inspect} doc count:#{child_docs.count}"
  abort("warning: no child docs found with key #{child_key.inspect} - exit") if child_docs.empty?
  child_docs_by_key = child_docs.collect{|doc| [doc[child_key], doc]}
  child_docs_by_key.sort!{|a,b| a.first <=> b.first}
  child_groups = ordered_group_by_first(child_docs_by_key)
  puts "info: child #{child_name.inspect} group count:#{child_groups.count}"
  ids = child_groups.collect{|group| group.first}
  parent_docs = parent_coll.find({'_id' => {'$in' => ids}}).to_a
  hash_parent_docs = hash_by_key(parent_docs, '_id')
  count = 0
  bulk = parent_coll.initialize_unordered_bulk_op
  child_groups.each do |group|
    key = group.first
    doc = hash_parent_docs[key] # nil check(?)
    doc[parent_key] = group.last # nil[] will fail
    bulk.find({'_id' => key}).replace_one(doc)
    count += 1
  end
  bulk.execute if count > 0
else
  print "info: progress - "
  parent_coll.find.each_slice(SLICE_SIZE) do |parent_docs|
    bulk = parent_coll.initialize_unordered_bulk_op
    count = 0
    ids = parent_docs.collect{|doc| doc['_id']}
    child_docs = child_coll.find({child_key => {'$in' => ids}}).to_a
    next if child_docs.empty?
    child_docs_by_key = child_docs.collect{|doc| [doc[child_key], doc]}
    child_docs_by_key.sort!{|a,b| a.first <=> b.first}
    #puts "debug: child #{child_name.inspect} slice doc count:#{child_docs_by_key.count}"
    child_groups = ordered_group_by_first(child_docs_by_key)
    #puts "debug: child #{child_name.inspect} slice group count:#{child_groups.count}"
    print child_groups.count
    hash_parent_docs = hash_by_key(parent_docs, '_id')
    child_groups.each do |group|
      key = group.first
      doc = hash_parent_docs[key] # nil check(?)
      doc[parent_key] = group.last # nil[] will fail
      bulk.find({'_id' => key}).replace_one(doc)
      count += 1
    end
    bulk.execute if count > 0
    putc('.')
  end
  puts
end

