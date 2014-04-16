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

module MongoMerge
  class Child
    attr_reader :child_count

    def initialize(db, child_name, child_key)
      @child_name = child_name
      @child_key = child_key
      @child_coll = db[@child_name]
      @child_count = @child_coll.count
      puts "info: child #{@child_name.inspect}, count: #{@child_count}"
      @child_coll.ensure_index(child_key => Mongo::ASCENDING)
    end
  end

  class Child1 < Child
    THRESHOLD = 80000 # 100000 fails in hash_by_key with stack level too deep (SystemStackError)

    attr_reader :child_key

    def initialize(db, child_name, child_key)
      super
      @child_hash = Hash.new
      @parent_docs_fetched = nil
    end

    def load_child_hash(parent_docs, parent_key)
      child_docs = if @child_count <= THRESHOLD
                     @child_coll.find({@child_key => {'$ne' => nil}}).to_a
                   else
                     keys = parent_docs.collect{|doc| val = doc[parent_key]; val.is_a?(Hash) ? val[@child_key] : val }.sort.uniq
                     @child_coll.find({@child_key => {'$in' => keys}}).to_a
                   end
      print "<#{child_docs.count}"
      @child_hash = hash_by_key(child_docs, @child_key)
    end

    def child_fetch(key, parent_docs, parent_key)
      doc = @child_hash[key]
      return doc if doc || parent_docs == @parent_docs_fetched
      load_child_hash(parent_docs, parent_key)
      @parent_docs_fetched = parent_docs
      @child_hash[key]
    end
  end

  class Combinator
    RE_PARENT_KEY = '(?<parent_key>[^:]+)'
    RE_CHILD_NAME = '(?<child_name>[^.\\[\\]]*)?'
    RE_CHILD_KEY = '(?<child_key>[^\\[\\]]*)'

    MERGED_NAME = 'merged'

    SLICE_SIZE = 10_000
    BATCH_SIZE = 5 * SLICE_SIZE

    def initialize(parent_name, merge_spec)
      @parent_name = parent_name
      @exanded_spec = merge_spec.collect do |child_spec|
        if (match_data = /^#{RE_PARENT_KEY}(:#{RE_CHILD_NAME}(\.#{RE_CHILD_KEY})?)?$/.match(child_spec))
          parent_key = match_data[:parent_key]
          child_name = match_data[:child_name] || parent_key
          child_name = parent_key if child_name.empty?
          child_key = match_data[:child_key] || '_id'
          child_key = '_id' if child_key.empty?
          [:one, parent_key, child_name, child_key]
        elsif (match_data = /^#{RE_PARENT_KEY}:\[#{RE_CHILD_NAME}(\.#{RE_CHILD_KEY})?\]$/.match(child_spec))
          parent_key = match_data[:parent_key]
          child_name = match_data[:child_name] || parent_key
          child_name = parent_key if child_name.empty?
          child_key = match_data[:child_key] || @parent_name
          child_key = @parent_name if child_key.empty?
          [:many, parent_key, child_name, child_key]
        else
          raise "unrecognized merge spec:#{child.inspect}"
        end
      end
    end

    def copy_one_with_parent_id(parent_key, child_name, child_key)
      child_coll = @db[child_name]
      h = hash_by_key(child_coll.find({child_key => {'$ne' => nil}}).to_a, child_key)
      @parent_coll.find({parent_key => {'$ne' => nil}}, :fields => {'_id' => 1, parent_key => 1}, :batch_size => BATCH_SIZE).each_slice(SLICE_SIZE) do |slice|
        bulk = @temp_coll.initialize_unordered_bulk_op
        slice.each do |doc|
          bulk.insert({'parent_id' => doc['_id'], parent_key => h[doc[parent_key]]})
        end
        bulk.execute
      end
    end

    def copy_many_with_parent_id(parent_key, child_name, child_key)
      child_coll = @db[child_name]
      child_coll.find({child_key => {'$ne' => nil}}, :batch_size => BATCH_SIZE).each_slice(SLICE_SIZE) do |slice|
        bulk = @temp_coll.initialize_unordered_bulk_op
        slice.each do |doc|
          bulk.insert({'parent_id' => doc[child_key], parent_key => doc})
        end
        bulk.execute
      end
    end

    def group_and_update(group_spec)
      pipeline = [{'$group' => group_spec}]
      @temp_coll.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).each_slice(SLICE_SIZE) do |slice| # :batch_size => BATCH_SIZE
        bulk = @parent_coll.initialize_unordered_bulk_op
        slice.each do |doc|
          id = doc['_id']
          doc.delete('_id')
          bulk.find({'_id' => id}).update_one({'$set' => doc})
        end
        bulk.execute
      end
    end

    def execute
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client.db
      merged_coll = @db[MERGED_NAME]
      merge_stamp = @parent_name
      if merged_coll.find({merged: merge_stamp}).to_a.empty?
        @parent_coll = @db[@parent_name]
        temp_name = "#{@parent_name}_merge_temp"
        @temp_coll = @db[temp_name]
        group_spec = {'_id' => '$parent_id'}
        @exanded_spec.each do |x, parent_key, child_name, child_key|
          if x == :one
            copy_one_with_parent_id(parent_key, child_name, child_key)
            group_spec.merge!(parent_key => {'$first' => "$#{parent_key}"})
          elsif x == :many
            copy_many_with_parent_id(parent_key, child_name, child_key)
            group_spec.merge!(parent_key => {'$push' => "$#{parent_key}"})
          else
            raise "not reached"
          end
        end
        group_and_update(group_spec)
        @db.drop_collection(temp_name)
        merged_coll.insert({merged: merge_stamp})
      else
        puts "info: merge skipped - already stamped in collection 'merged'"
      end
      @mongo_client.close
    end
  end
end

if $0 == __FILE__
  USAGE = <<-EOT.gsub('    ', '')
    usage: MONGODB_URI='mongodb://localhost:27017/database_name' #{$0} parent_collection child_spec ...
    where child_spec is parent_key:child_collection.child_key for one to one merge
    with parent_key as default child_collection and '_id' as default child_key
    or where child_spec is parent_key:[child_collection.child_key] for one to many merge
    with parent_key as default child_collection and parent_collection as default child_key
  EOT
  abort(USAGE) if ARGV.size < 2
  puts ARGV.join(' ')
  parent_name, merge_spec = ARGV.shift
  combinator = MongoMerge::Combinator.new(parent_name, merge_spec)
  doc_count = 0
  tms = Benchmark.measure do
    doc_count = combinator.execute
  end
  puts "info: real: #{'%.2f' % tms.real}, user: #{'%.2f' % tms.utime}, system:#{'%.2f' % tms.stime}, docs_per_sec: #{(doc_count.to_f/[tms.real, 0.000001].max).round}"
end
