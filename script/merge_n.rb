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

def ordered_group_by_first(pairs)
  pairs.inject([[], nil]) do |memo, pair|
    result, previous_value = memo
    current_value = pair.first
    result << [current_value, []] if previous_value != current_value
    result.last.last << pair.last
    [result, current_value]
  end.first
end

def merge_n_batch(parent_coll, parent_key, child_groups)
  count = 0
  bulk = parent_coll.initialize_unordered_bulk_op
  child_groups.each do |group|
    key = group.first
    bulk.find({'_id' => key}).update_one({'$set' => {parent_key => group.last}})
    count += 1
  end
  bulk.execute if count > 0
  print ">#{count}"
end

mongo_client = Mongo::MongoClient.from_uri
mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
db = mongo_client[mongo_uri.db_name]

USAGE = "usage: #{$0} parent.foreign_key child.id"
abort(USAGE) if ARGV.size != 2
parent_arg = ARGV[0].split('.', -1)
child_arg = ARGV[1].split('.', -1)
abort(USAGE) if parent_arg.size != 2 || child_arg.size != 2

parent_name, parent_key = parent_arg
parent_coll = db[parent_name]
parent_count = parent_coll.count
puts "info: parent #{parent_name.inspect} count: #{parent_count}"

child_name, child_key = child_arg
child_coll = db[child_name]
child_count = child_coll.count
puts "info: child #{child_name.inspect} count: #{child_count}"
child_coll.ensure_index(child_key => Mongo::ASCENDING)

THRESHOLD = 10000
SLICE_SIZE = 10000

doc_count = 0
bm = Benchmark.measure do
  if child_count <= THRESHOLD
    child_docs = child_coll.find({child_key => {'$exists' => true}}).to_a
    puts "info: child:#{child_name.inspect} key:#{child_key.inspect} count:#{child_docs.count}"
    abort("warning: no docs found for child:#{child_name.inspect} key:#{child_key.inspect} - exit") if child_docs.empty?
    child_docs_by_key = child_docs.collect{|doc| [doc[child_key], doc]}
    child_docs_by_key.sort!{|a,b| a.first <=> b.first}
    child_groups = ordered_group_by_first(child_docs_by_key)
    puts "info: child:#{child_name.inspect} group count:#{child_groups.count}"
    print "info: progress: "
    merge_n_batch(parent_coll, parent_key, child_groups)
  else
    puts "info: ******** over #{THRESHOLD} threshold ********"
    print "info: progress: "
    parent_coll.find({}, :fields => {'_id' => 1}).each_slice(SLICE_SIZE) do |parent_docs|
      doc_count += parent_docs.size
      ids = parent_docs.collect{|doc| doc['_id']}
      child_docs = child_coll.find({child_key => {'$in' => ids}}).to_a
      putc('.')
      STDOUT.flush
      next if child_docs.empty?
      child_docs_by_key = child_docs.collect{|doc| [doc[child_key], doc]}
      child_docs_by_key.sort!{|a,b| a.first <=> b.first}
      #puts "debug: child #{child_name.inspect} slice doc count:#{child_docs_by_key.count}"
      child_groups = ordered_group_by_first(child_docs_by_key)
      #puts "debug: child #{child_name.inspect} slice group count:#{child_groups.count}"
      merge_n_batch(parent_coll, parent_key, child_groups)
    end
  end
  puts
end
puts "info: real: #{'%.2f' % bm.real}, user: #{'%.2f' % bm.utime}, system:#{'%.2f' % bm.stime}, docs_per_sec: #{(doc_count.to_f/[bm.real, 0.000001].max).round}"
mongo_client.close

