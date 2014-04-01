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
require 'merge_n'

MONGODB_URI = 'mongodb://localhost:27017/test_merge_n'
ENV['MONGODB_URI'] = MONGODB_URI
MONGO_URI = Mongo::URIParser.new(ENV['MONGODB_URI'])
DB_NAME = MONGO_URI.db_name

describe Mongo::Combinator do

  before(:each) do
    @mongo_client = Mongo::MongoClient.from_uri
    @db = @mongo_client[DB_NAME]
    @combinator = Mongo::Combinator.new(@db, 'owner', 'pet', 'pet', 'owner')
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
            ]
        },
        :after => {
            :owner => [
                {"_id" => 11, "name" => "Joe",
                 "pet" => [
                     {"_id" => 1, "name" => "Lassie", "owner" => 11}
                 ]
                },
                {"_id" => 22, "name" => "Jane",
                 "pet" => [
                     {"_id" => 2, "name" => "Flipper", "owner" => 22},
                     {"_id" => 3, "name" => "Snoopy", "owner" => 22}
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

  context "combinator" do
    it("should merge child into parent") {
      @combinator.merge_n # initial merge_n
      match_fixture(@db, @data[:after])
    }
    it("should re-merge child into parent") {
      @combinator.merge_n # initial merge_n
      @combinator.merge_n # re-run merge_n
      match_fixture(@db, @data[:after])
    }
  end

  context "ordered_group_by_first" do
    it("should order group by first element") {
      pairs = [
          ["cat", "Garfield"], ["cat", "Midnight"],
          ["dog", "Snoopy"], ["dog", "Maramduke"],
          ["rabbit", "Flopsy"], ["rabbit", "Mopsy"]
      ]
      result = [
          ["cat", ["Garfield", "Midnight"]],
          ["dog", ["Maramduke", "Snoopy"]],
          ["rabbit", ["Flopsy", "Mopsy"]]
      ]
      expect(ordered_group_by_first(pairs)).to eq(result)
    }
  end
end

