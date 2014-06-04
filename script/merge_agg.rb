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

require 'mongo'
require 'benchmark'
require 'pp'

module MongoMerge
  class Combinator

    RE_PARENT_KEY = '(?<parent_key>[^:]+)'
    RE_CHILD_NAME = '(?<child_name>[^.\\[\\]]*)?'
    RE_CHILD_KEY = '(?<child_key>[^\\[\\]]*)'

    SLICE_SIZE = 20_000
    BATCH_SIZE = 5 * SLICE_SIZE

    def initialize

    end

    def child_by_merge_key(parent_key, child_name, child_key)
      [
          {'$project' => {
              '_id' => 0, 'child_name' => {'$literal' => child_name},
              'merge_id' => "$#{child_key}",
              parent_key => '$$ROOT'}
          }
      ]
    end

    def parent_child_merge_key(parent_key, child_name, child_key)
      [
          {'$project' => {
              '_id' => 0, 'child_name' => {'$literal' => child_name},
              'merge_id' => {'$ifNull' => ["$#{parent_key}.#{child_key}", "$#{parent_key}"]},
              'parent_id' => "$_id"}
          }
      ]
    end

    def merge_one_all(accumulators, projectors)
      [
          {'$group' => {
              '_id' => {'child_name' => '$child_name', 'merge_id' => '$merge_id'},
              'parent_id' => {'$push' => '$parent_id'}}.merge(accumulators)},
          {'$unwind' => '$parent_id'},
          {'$group' => {
              '_id' => '$parent_id'}.merge(accumulators)},
          {'$project' => {'_id' => 0, 'parent_id' => '$_id'}.merge(projectors)}
      ]
    end

    def copy_many_with_parent_id(parent_key, child_name, child_key)
      [
          {'$match' => {child_key => {'$ne' => nil}}},
          {'$project' => {'_id' => 0, 'parent_id' => "$#{child_key}", parent_key => '$$ROOT'}}
      ]
    end

    def agg_copy(source_coll, dest_coll, pipeline)
      source_coll.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).each_slice(SLICE_SIZE) do |docs|
        bulk = dest_coll.initialize_unordered_bulk_op
        docs.each{|doc| bulk.insert(doc)}
        begin
          bulk.execute
        rescue => ex
          puts "agg_copy exception: #{ex.inspect}"
          raise ex
        end
        print ">#{docs.count}"
        STDOUT.flush
      end
    end

    def group_and_update(source_coll, dest_coll, accumulators)
      doc_count = 0
      pipeline = [{'$group' => {'_id' => '$parent_id'}.merge(accumulators)}]
      source_coll.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).each_slice(SLICE_SIZE) do |temp_docs| # :batch_size => BATCH_SIZE
        count = 0
        bulk = dest_coll.initialize_unordered_bulk_op
        temp_docs.each do |doc|
          id = doc.delete('_id')
          doc = doc.select{|key, value| !value.nil? && !value.empty?}
          unless doc.empty?
            bulk.find({'_id' => id}).update_one({'$set' => doc})
            count += 1
          end
        end
        begin
          bulk.execute if count > 0
        rescue => ex
          puts "group_and_update exception: #{ex.inspect}"
          raise ex
        end
        print ">#{count}"
        STDOUT.flush
        doc_count += count
      end
      doc_count
    end

    def expand_spec(parent_name, merge_spec, one_spec, many_spec)
      merge_spec.collect do |child_spec|
        if (match_data = /^#{RE_PARENT_KEY}(:#{RE_CHILD_NAME}(\.#{RE_CHILD_KEY})?)?$/.match(child_spec))
          parent_key = match_data[:parent_key]
          child_name = match_data[:child_name] || parent_key
          child_name = parent_key if child_name.empty?
          child_key = match_data[:child_key] || '_id'
          child_key = '_id' if child_key.empty?
          one_spec << [:one, parent_key, child_name, child_key]
        elsif (match_data = /^#{RE_PARENT_KEY}:\[#{RE_CHILD_NAME}(\.#{RE_CHILD_KEY})?\]$/.match(child_spec))
          parent_key = match_data[:parent_key]
          child_name = match_data[:child_name] || parent_key
          child_name = parent_key if child_name.empty?
          child_key = match_data[:child_key] || parent_name
          child_key = parent_name if child_key.empty?
          many_spec << [:many, parent_key, child_name, child_key]
        else
          raise "unrecognized merge spec:#{child.inspect}"
        end
      end
    end

    def one_children_append(parent_name, one_spec, db, parent_coll, temp_coll, all_accumulators)
      temp_one_name = "#{parent_name}_merge_temp_one"
      db.drop_collection(temp_one_name)
      temp_one_coll = db[temp_one_name]
      one_accumulators = {}
      one_projectors = {}

      one_spec.each do |spec|
        x, parent_key, child_name, child_key = spec
        puts "info: spec: #{spec.inspect}"
        print "info: progress: "
        child_coll = db[child_name]
        agg_copy(child_coll, temp_one_coll, child_by_merge_key(parent_key, child_name, child_key))
        agg_copy(parent_coll, temp_one_coll, parent_child_merge_key(parent_key, child_name, child_key))
        all_accumulators.merge!(parent_key => {'$max' => "$#{parent_key}"})
        one_accumulators.merge!(parent_key => {'$max' => "$#{parent_key}"})
        one_projectors.merge!(parent_key => "$#{parent_key}")
        puts
      end
      agg_copy(temp_one_coll, temp_coll, merge_one_all(one_accumulators, one_projectors))

      db.drop_collection(temp_one_name)
    end

    def many_children_append(parent_name, many_spec, db, temp_coll, all_accumulators)
      many_spec.each do |spec|
        x, parent_key, child_name, child_key = spec
        puts "info: spec: #{spec.inspect}"
        print "info: progress: "
        child_coll = db[child_name]
        agg_copy(child_coll, temp_coll, copy_many_with_parent_id(parent_key, child_name, child_key))
        all_accumulators.merge!(parent_key => {'$push' => "$#{parent_key}"})
        puts
      end
    end

    def execute(parent_name, merge_spec)
      one_spec = []
      many_spec = []

      expand_spec(parent_name, merge_spec, one_spec, many_spec)

      mongo_client = Mongo::MongoClient.from_uri
      db = mongo_client.db

      parent_coll = db[parent_name]
      temp_name = "#{parent_name}_merge_temp"
      db.drop_collection(temp_name)
      temp_coll = db[temp_name]
      all_accumulators = {}

      one_children_append(parent_name, one_spec, db, parent_coll, temp_coll, all_accumulators)
      many_children_append(parent_name, many_spec, db, temp_coll, all_accumulators)

      puts "info: group: #{parent_name}"
      print "info: progress: "
      STDOUT.flush
      doc_count = group_and_update(temp_coll, parent_coll, all_accumulators)
      puts

      db.drop_collection(temp_name)
      mongo_client.close
      doc_count
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
  parent_name, *merge_spec = ARGV
  combinator = MongoMerge::Combinator.new
  doc_count = 0
  tms = Benchmark.measure do
    doc_count = combinator.execute(parent_name, merge_spec)
  end
  puts "info: real: #{'%.2f' % tms.real}, user: #{'%.2f' % tms.utime}, system:#{'%.2f' % tms.stime}, docs_per_sec: #{(doc_count.to_f/[tms.real, 0.000001].max).round}"
end
