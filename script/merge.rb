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

def hash_by_key(a, key)
  Hash[*a.collect{|e| [e[key], e]}.flatten(1)]
end

def ordered_group_by_first(pairs, child_key = nil)
  pairs.inject([[], nil]) do |memo, pair|
    result, previous_value = memo
    current_value = pair.first
    if previous_value != current_value
      if child_key && result.last && (obj = result.last.last)
        if obj.first.is_a?(Hash)
          obj.sort!{|a,b| a[child_key] <=> b[child_key]}
        else
          obj.sort!{|a,b| a <=> b}
        end
      end
      result << [current_value, []]
    end
    result.last.last << pair.last
    [result, current_value]
  end.first
end

module Mongo
  class Combinator1
    SLICE_SIZE = 20000
    THRESHOLD = 80000 # 100000 fails in hash_by_key with stack level too deep (SystemStackError)

    def initialize(db, parent_name, parent_key, child_name, child_key)
      @parent_name = parent_name
      @parent_key = parent_key
      @parent_coll = db[@parent_name]
      puts "info: parent #{parent_name.inspect}, count: #{@parent_coll.count}"
      @child_name = child_name
      @child_key = child_key
      @child_coll = db[@child_name]
      @child_count = @child_coll.count
      puts "info: child #{@child_name.inspect}, count: #{@child_count}"
      @child_coll.ensure_index(child_key => Mongo::ASCENDING)

      @child_hash = Hash.new
      @parent_docs_fetched = nil
    end

    def load_child_hash(parent_docs)
      child_docs = if @child_count <= THRESHOLD
                     @child_coll.find({@child_key => {'$ne' => nil}}).to_a
                   else
                     keys = parent_docs.collect{|doc| val = doc[@parent_key]; val.is_a?(Hash) ? val[@child_key] : val }.sort.uniq
                     @child_coll.find({@child_key => {'$in' => keys}}).to_a
                   end
      print "<#{child_docs.count}"
      @child_hash = hash_by_key(child_docs, @child_key)
    end

    def child_fetch(key, parent_docs)
      doc = @child_hash[key]
      return doc if doc || parent_docs == @parent_docs_fetched
      load_child_hash(parent_docs)
      @parent_docs_fetched = parent_docs
      @child_hash[key]
    end

    def merge_1_batch(parent_docs)
      count = 0
      bulk = @parent_coll.initialize_unordered_bulk_op
      parent_docs.each do |doc|
        val = doc[@parent_key]
        next unless val
        fk = val.is_a?(Hash) ? val[@child_key] : val
        abort("abort: #{$0} #{ARGV.join(' ')} - line:#{__LINE__} - expected child key #{@child_key.inspect} to reapply merge - val:#{val.inspect} - exit") unless fk
        child_doc = child_fetch(fk, parent_docs)
        puts("warning: #{$0} #{ARGV.join(' ')} - line:#{__LINE__} - unexpected fk:#{fk.inspect} - continuing") unless child_doc
        next unless child_doc
        bulk.find({'_id' => doc['_id']}).update_one({'$set' => {@parent_key => child_doc}})
        count += 1
      end
      bulk.execute if count > 0
      print ">#{count}"
    end

    def merge_1
      doc_count = 0
      print "info: progress: "
      @parent_coll.find({@parent_key => {'$ne' => nil}}, :fields => {'_id' => 1, @parent_key => 1}).each_slice(SLICE_SIZE) do |parent_docs|
        doc_count += parent_docs.size
        merge_1_batch(parent_docs)
        putc('.')
        STDOUT.flush
      end
      puts
      doc_count
    end
  end

  class CombinatorN
    SLICE_SIZE = 20000
    THRESHOLD = 1000000

    def initialize(db, parent_name, parent_key, child_name, child_key)
      @parent_name = parent_name
      @parent_key = parent_key
      @parent_coll = db[@parent_name]
      puts "info: parent #{parent_name.inspect}, count: #{@parent_coll.count}"
      @child_name = child_name
      @child_key = child_key
      @child_coll = db[@child_name]
      @child_count = @child_coll.count
      puts "info: child #{@child_name.inspect}, count: #{@child_count}"
      @child_coll.ensure_index(child_key => Mongo::ASCENDING)
    end

    def load_child_groups(parent_docs = nil)
      child_docs = if @child_count <= THRESHOLD
                     @child_coll.find({@child_key => {'$ne' => nil}}).to_a
                   else
                     keys = parent_docs.collect{|doc| doc['_id']}
                     @child_coll.find({@child_key => {'$in' => keys}}).to_a
                   end
      child_docs_by_key = child_docs.collect{|doc| [doc[@child_key], doc]}
      child_docs_by_key.sort!{|a,b| a.first <=> b.first}
      child_groups = ordered_group_by_first(child_docs_by_key) # no @child_key sort for now
      print "<#{child_docs.size}~#{child_groups.size}"
      child_groups
    end

    def merge_n_batch(child_groups)
      count = 0
      bulk = @parent_coll.initialize_unordered_bulk_op
      child_groups.each do |group|
        key = group.first
        bulk.find({'_id' => key}).update_one({'$set' => {@parent_key => group.last}})
        count += 1
      end
      bulk.execute if count > 0
      print ">#{count}"
    end

    def merge_n_small
      child_groups = load_child_groups
      merge_n_batch(child_groups)
      child_groups.size
    end

    def merge_n_big
      doc_count = 0
      @parent_coll.find({}, :fields => {'_id' => 1}).each_slice(SLICE_SIZE) do |parent_docs|
        doc_count += parent_docs.size
        child_groups = load_child_groups(parent_docs)
        merge_n_batch(child_groups)
        putc('.')
        STDOUT.flush
      end
      doc_count
    end

    def merge_n
      doc_count = 0
      if @child_count <= THRESHOLD
        puts "info: under THRESHOLD #{THRESHOLD}"
        print "info: progress: "
        doc_count = merge_n_small
      else
        puts "info: over THRESHOLD #{THRESHOLD}"
        print "info: progress: "
        doc_count = merge_n_big
      end
      puts
      doc_count
    end
  end
end

if $0 == __FILE__
  USAGE = "usage: MONGODB_URI='mongodb://localhost:27017/database_name' #{$0} parent.foreign_key child.primary_key"
  abort(USAGE) if ARGV.size != 2
  parent_name, parent_key = ARGV[0].split('.', 2)
  child_name, child_key = ARGV[1].split('.', 2)
  abort(USAGE) unless parent_name && parent_key && child_name && child_key

  mongo_client = Mongo::MongoClient.from_uri
  mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
  db = mongo_client[mongo_uri.db_name]
  combinator = Mongo::Combinator1.new(db, parent_name, parent_key, child_name, child_key)

  doc_count = 0
  bm = Benchmark.measure do
    doc_count = combinator.merge_1
  end
  puts "info: real: #{'%.2f' % bm.real}, user: #{'%.2f' % bm.utime}, system:#{'%.2f' % bm.stime}, docs_per_sec: #{(doc_count.to_f/[bm.real, 0.000001].max).round}"
  mongo_client.close
end
