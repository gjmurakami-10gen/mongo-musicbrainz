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
require 'mbdump_to_mongo'
require 'pp'

describe "load_table" do

  context "load_table" do

    before(:each) do
      @mongodb_uri = 'mongodb://localhost:27017/test_mbdump_to_mongo'
      ENV['MONGODB_URI'] = @mongodb_uri
      @mongo_client = Mongo::MongoClient.from_uri
      @db = @mongo_client.db
    end

    after(:each) do
      @mongo_client.drop_database(@db.name)
    end

    it("should get columns") {
      table_name = 'work'
      columns = get_columns(table_name)
      column_names = columns.collect{|e| e['column_name']}
      expect(column_names).to eq(["id", "gid", "name", "type", "comment", "edits_pending", "last_updated", "language"])
    }

    it("should merge transforms") {
      table_name = 'work'
      columns = get_columns(table_name)
      columns = merge_transforms(columns)
      classes = columns.collect{|c| c['transform'].class}
      expect(classes).to eq([Proc, NilClass, NilClass, Proc, NilClass, Proc, Proc, Proc])
    }

  end
end

