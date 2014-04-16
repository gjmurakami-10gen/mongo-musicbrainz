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

require_relative 'spec_helper'
require 'merge'

unless defined? Mongo::ObjectId.<=>
  module BSON
    class ObjectId
      def <=> (other) #1 if self>other; 0 if self==other; -1 if self<other
        self.data <=> other.data
      end
    end
  end
end

describe MongoMerge::Combinator1 do

  context "combinator1" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_merge_1'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
      @db_name = @mongo_uri.db_name

      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client[@db_name]
      @combinator = MongoMerge::Combinator1.new(@db, 'people', 'gender', 'gender', '_id')
      @data = {
          :before => {
              :people => [
                  {"_id" => 11, "name" => "Joe", "gender" => 1},
                  {"_id" => 22, "name" => "Jane", "gender" => 2},
                  {"_id" => 33, "name" => "Other"}
              ],
              :gender => [
                  {"_id" => 1, "name" => "Male"},
                  {"_id" => 2, "name" => "Female"},
                  {"_id" => 3, "name" => "Other"}
              ]
          },
          :after => {
              :people => [
                  {"_id"=>11, "name"=>"Joe", "gender"=>{"_id"=>1, "name"=>"Male"}},
                  {"_id"=>22, "name"=>"Jane", "gender"=>{"_id"=>2, "name"=>"Female"}},
                  {"_id"=>33, "name"=>"Other"}
              ]
          }
      }
      load_fixture(@db, @data[:before])
    end

    after(:each) do
      @mongo_client.drop_database(@db_name)
    end

    it("should merge child into parent") {
      @combinator.merge_1 # initial merge_1
      match_fixture(@db, @data[:after])
    }
    it("should re-merge child into parent") {
      @combinator.merge_1 # initial merge_1
      @combinator.merge_1 # re-run merge_1
      match_fixture(@db, @data[:after])
    }

  end

  context "combinatorN" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_merge_n'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_uri = Mongo::URIParser.new(ENV['MONGODB_URI'])
      @db_name = @mongo_uri.db_name

      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client[@db_name]
      @combinator = MongoMerge::CombinatorN.new(@db, 'owner', 'pet', 'pet', 'owner')
      @data = {
          :before => {
              :owner => [
                  {"_id" => 11, "name" => "Joe"},
                  {"_id" => 22, "name" => "Jane"},
                  {"_id" => 33, "name" => "Jack"},
                  {"_id" => 44, "name" => "Other"}
              ],
              :pet => [
                  {"_id" => 1, "name" => "Lassie", "owner" => 11},
                  {"_id" => 2, "name" => "Flipper", "owner" => 22},
                  {"_id" => 3, "name" => "Snoopy", "owner" => 22},
                  {"_id" => 4, "name" => "Garfield", "owner" => 33},
                  {"_id" => 5, "name" => "Marmaduke"}
              ],
              :alias => [
                  {"_id" => 1, "name" => "Joseph", "owner" => 11},
                  {"_id" => 2, "name" => "Janey", "owner" => 22},
                  {"_id" => 3, "name" => "JJ", "owner" => 22},
                  {"_id" => 4, "name" => "John", "owner" => 33},
                  {"_id" => 5, "name" => "Jim"}
              ]
          },
          :after => {
              :owner => [
                  {"_id" => 11, "name" => "Joe",
                   "pet" => [
                       {"_id" => 1, "name" => "Lassie", "owner" => 11}
                   ],
                   "alias" => [
                       {"_id" => 1, "name" => "Joseph", "owner" => 11}
                   ]
                  },
                  {"_id" => 22, "name" => "Jane",
                   "pet" => [
                       {"_id" => 2, "name" => "Flipper", "owner" => 22},
                       {"_id" => 3, "name" => "Snoopy", "owner" => 22}
                   ],
                   "alias" => [
                       {"_id" => 2, "name" => "Janey", "owner" => 22},
                       {"_id" => 3, "name" => "JJ", "owner" => 22},
                   ]
                  },
                  {"_id" => 33, "name" => "Jack",
                   "pet" => [
                       {"_id" => 4, "name" => "Garfield", "owner" => 33}
                   ],
                   "alias" => [
                       {"_id" => 4, "name" => "John", "owner" => 33},
                   ]
                  },
                  {"_id" => 44, "name" => "Other"}
              ]
          }
      }
      load_fixture(@db, @data[:before])
    end

    after(:each) do
      @mongo_client.drop_database(@db_name)
    end

    it("should sort BSON::OrderedHash") {
      a = [
          BSON::OrderedHash["_id", BSON::ObjectId.new, "name", "Flopsy"],
          BSON::OrderedHash["_id", BSON::ObjectId.new, "name", "Mopsy"],
      ]
      expect(a.sort!{|a,b| a.first.last <=> b.first.last}).to eq(a)
    }

    it("should merge children into parent") {
      @combinator.merge_n # initial merge_n
      @combinator = MongoMerge::CombinatorN.new(@db, 'owner', 'alias', 'alias', 'owner')
      @combinator.merge_n # initial merge_n
      match_fixture(@db, @data[:after])
    }
    it("should re-merge children into parent") {
      @combinator.merge_n # initial merge_n
      @combinator.merge_n # re-run merge_n
      @combinator = MongoMerge::CombinatorN.new(@db, 'owner', 'alias', 'alias', 'owner')
      @combinator.merge_n # re-run merge_n
      @combinator.merge_n # re-run merge_n
      match_fixture(@db, @data[:after])
    }
    it("should merge children into parent using aggregation") {
      spec = [
       ['pet', 'pet', 'owner'],
       ['alias', 'alias', 'owner']
      ]
      coll_tmp = @db['agg_tmp']
      spec.each do |parent_key, child_name, child_key|
        coll = @db[child_name]
        coll.find({child_key => {'$ne' => nil}}).each_slice(10_000) do |slice|
          bulk = coll_tmp.initialize_unordered_bulk_op
          slice.each do |doc|
            bulk.insert({'parent_id' => doc[child_key], parent_key => doc})
          end
          bulk.execute
        end
      end
      fields = spec.collect{|a|a.first}
      group = Hash[*(['_id', '$parent_id'] + fields.collect{|k|[k,{'$push' => "$#{k}"}]}).flatten]
      pipeline = [{'$group' => group}]
      @coll = @db['owner']
      coll_tmp.aggregate(pipeline, :cursor => {}, :allowDiskUse => true).each_slice(10_000) do |slice|
        bulk = @coll.initialize_unordered_bulk_op
        slice.each do |doc|
          id = doc['_id']
          doc.delete('_id')
          bulk.find({'_id' => id}).update({'$set' => doc})
        end
        bulk.execute
      end
      match_fixture(@db, @data[:after])
    }
  end
end

