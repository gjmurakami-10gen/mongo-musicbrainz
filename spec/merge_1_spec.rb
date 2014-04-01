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
require 'merge_1'

MONGODB_URI = 'mongodb://localhost:27017/test_merge_1'
ENV['MONGODB_URI'] = MONGODB_URI
MONGO_URI = Mongo::URIParser.new(ENV['MONGODB_URI'])
DB_NAME = MONGO_URI.db_name

describe Mongo::Combinator do

  context "combinator" do

    before(:each) do
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client[DB_NAME]
      @combinator = Mongo::Combinator.new(@db, 'people', 'gender', 'gender', '_id')
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
end

