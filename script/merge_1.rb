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
require 'pp'
require 'json'
require 'mongo'
require 'benchmark'
require 'ruby-prof'

def hash_by_key(a, key)
  Hash[*a.collect{|e| [e[key], e]}.flatten(1)]
end

def bulk_merge(parent_docs, parent_key, child_docs_hash, child_key, parent_coll)
  count = 0
  bulk = parent_coll.initialize_unordered_bulk_op
  parent_docs.each do |doc|
    val = doc[parent_key]
    next unless val
    fk = val.is_a?(Hash) ? val[child_key] : val
    abort("abort: #{$0} #{ARGV.join(' ')} - line:#{__LINE__} - expected child key #{child_key.inspect} to reapply merge - val:#{val.inspect} - exit") unless fk
    child_doc = child_docs_hash[fk]
    puts("warning: #{$0} #{ARGV.join(' ')} - line:#{__LINE__} - unexpected fk:#{fk.inspect} - continuing") unless child_doc
    next unless child_doc
    bulk.find({'_id' => doc['_id']}).update_one({'$set' => {parent_key => child_doc}})
    count += 1
  end
  bulk.execute if count > 0
  count
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
parent_count = parent_coll.count
puts "info: parent #{parent_name.inspect} count: #{parent_count}"

child_name, child_key = child_arg
child_coll = $db[child_name]
child_count = child_coll.count
puts "info: child #{child_name.inspect} count: #{child_count}"

THRESHOLD = 10000
SLICE_SIZE = 10000

doc_count = 0
bm = Benchmark.measure do
  if child_count <= THRESHOLD
    child_docs = child_coll.find({child_key => {'$exists' => true}}).to_a
    puts "info: child #{child_name.inspect} key #{child_key.inspect} count:#{child_docs.count}"
    abort("warning: no docs found for child #{child_name.inspect} key #{child_key.inspect} - exit") if child_docs.empty?
    child_docs_hash = hash_by_key(child_docs, child_key)
    parent_coll.find({parent_key => {'$exists' => true}}, :fields => {'_id' => 1, parent_key => 1}).each_slice(SLICE_SIZE) do |parent_docs|
      doc_count += parent_docs.size
      bulk_merge(parent_docs, parent_key, child_docs_hash, child_key, parent_coll)
    end
  else
    puts "info: ******** over #{THRESHOLD} threshold ********"
    print "info: progress: "
    parent_coll.find({parent_key => {'$exists' => true}}, :fields => {'_id' => 1, parent_key => 1}).each_slice(SLICE_SIZE) do |parent_docs|
      doc_count += parent_docs.size
      keys = parent_docs.collect{|doc| val = doc[parent_key]; val.is_a?(Hash) ? val[child_key] : val }.sort.uniq
      child_docs = child_coll.find({child_key => {'$in' => keys}}).to_a
      putc('.')
      STDOUT.flush
      next if child_docs.empty?
      child_docs_hash = hash_by_key(child_docs, child_key)
      count = bulk_merge(parent_docs, parent_key, child_docs_hash, child_key, parent_coll)
      print count
    end
    puts
  end
end
puts "docs_per_sec: #{(doc_count.to_f/[bm.real, 0.000001].max).round}"

