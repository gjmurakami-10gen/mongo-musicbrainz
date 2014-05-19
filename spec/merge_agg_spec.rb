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
require 'merge_agg'

unless defined? Mongo::ObjectId.<=>
  module BSON
    class ObjectId
      def <=> (other) #1 if self>other; 0 if self==other; -1 if self<other
        self.data <=> other.data
      end
    end
  end
end

describe MongoMerge::Combinator do

  context "combinator1" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_merge_1'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client.db
      @data = {
          :before => {
              :people => [
                  {"_id" => 11, "name" => "Joe", "gender" => 1, "alias" => 1},
                  {"_id" => 22, "name" => "Jane", "gender" => 2},
                  {"_id" => 33, "name" => "Other"}
              ],
              :gender => [
                  {"_id" => 1, "name" => "Male"},
                  {"_id" => 2, "name" => "Female"},
                  {"_id" => 3, "name" => "Other"}
              ],
              :alias => [
                  {"_id" => 1, "name" => "Joseph"}
              ]
          },
          :after => {
              :people => [
                  {"_id"=>11, "name"=>"Joe", "gender"=>{"_id"=>1, "name"=>"Male"}, "alias" => {"_id" => 1, "name" => "Joseph"}},
                  {"_id"=>22, "name"=>"Jane", "gender"=>{"_id"=>2, "name"=>"Female"}},
                  {"_id"=>33, "name"=>"Other"}
              ]
          }
      }
      load_fixture(@db, @data[:before])
    end

    after(:each) do
      @mongo_client.drop_database(@db.name)
    end

    it("should merge children into parent using aggregation") {
      combinator = MongoMerge::Combinator.new
      combinator.execute('people', ['gender', 'alias'])
      match_fixture(@db, @data[:after])
    }
    it("should remerge children into parent using aggregation") {
      combinator = MongoMerge::Combinator.new
      combinator.execute('people', ['gender', 'alias'])
      match_fixture(@db, @data[:after])
      @db.drop_collection('merged')
      combinator.execute('people', ['gender', 'alias'])
      match_fixture(@db, @data[:after])
    }

  end

  context "combinatorN" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_merge_n'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client.db
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
                       {"_id" => 3, "name" => "JJ", "owner" => 22}
                   ]
                  },
                  {"_id" => 33, "name" => "Jack",
                   "pet" => [
                       {"_id" => 4, "name" => "Garfield", "owner" => 33}
                   ]
                  },
                  {"_id" => 44, "name" => "Other"}
              ]
          }
      }
      load_fixture(@db, @data[:before])
    end

    after(:each) do
      @mongo_client.drop_database(@db.name)
    end

    it("should sort BSON::OrderedHash") {
      a = [
          BSON::OrderedHash["_id", BSON::ObjectId.new, "name", "Flopsy"],
          BSON::OrderedHash["_id", BSON::ObjectId.new, "name", "Mopsy"],
      ]
      expect(a.sort!{|a,b| a.first.last <=> b.first.last}).to eq(a)
    }

    it("should merge children into parent using aggregation") {
      combinator = MongoMerge::Combinator.new
      combinator.execute('owner', ['pet:[]', 'alias:[]'])
      match_fixture(@db, @data[:after])
    }
    it("should re-merge children into parent using aggregation") {
      combinator = MongoMerge::Combinator.new
      combinator.execute('owner', ['pet:[]', 'alias:[]'])
      match_fixture(@db, @data[:after])
      @db.drop_collection('merged')
      combinator.execute('owner', ['pet:[]', 'alias:[]'])
      match_fixture(@db, @data[:after])
    }

  end

  context "aggregation experiments" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_agg_exp'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client.db
    end

    after(:each) do
      @mongo_client.drop_database(@db.name)
    end

  end
end

