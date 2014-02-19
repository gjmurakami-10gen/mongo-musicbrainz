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

describe CreateTablesParser do
  let (:parser) { described_class.new }

  describe "space" do
    it("should parse white space") { parser.space.should parse("\t \n") }
  end

  describe "space?" do
    it("should parse white space") { parser.space?.should parse("\t \n") }
    it("should parse empty string") { parser.space?.should parse("") }
  end

  describe "directive" do
    directive = "\\set ON_ERROR_STOP 1"
    it("should parse example") { parser.directive.should parse(directive) }
  end

  describe "ident" do
    [ "id", "name", "sort_name", "NOW"
    ].each do |ident|
      it("should parse ident") { parser.ident.should parse(ident) }
      it("should parse ident") { parser.ident.should parse(ident) }
    end
  end

  describe "data_type" do
    [ "BOOLEAN", "INT", "INTEGER", "SERIAL", "SMALLINT", "TEXT", "TIMESTAMP", "UUID", "VARCHAR",
      "CHAR(16)", "VARCHAR(255)",
      "cover_art_presence", "timestamptz", "uuid",
      "INTEGER[]"
    ].each do |data_type|
      it("should parse data_type") { parser.data_type.should parse(data_type) }
    end
  end

  describe "not_parens" do
    it("should parse newline") { parser.not_parens.should parse("\n") }
  end

  $ended_check = <<-EOT.gsub(/^\s*/, '')
      (
        (
          -- If any end date fields are not null, then ended must be true
          (end_date_year IS NOT NULL OR
           end_date_month IS NOT NULL OR
           end_date_day IS NOT NULL) AND
          ended = TRUE
        ) OR (
          -- Otherwise, all end date fields must be null
          (end_date_year IS NULL AND
           end_date_month IS NULL AND
           end_date_day IS NULL)
        )
      )
  EOT

  describe "call_args" do
    it("should parse ()") { parser.call_args.should parse("()") }
    it("should parse (...)") { parser.call_args.should parse("(edits_pending >=0)") }
    it("should parse (...(...)...)") { parser.call_args.should parse($ended_check) }
  end

  describe "column_constraint" do
    [
      "NOT NULL", "NULL", "CHECK ( expression )",
      "DEFAULT TRUE", "DEFAULT FALSE", "DEFAULT true", "DEFAULT false",
      "DEFAULT 0", "DEFAULT -1", "DEFAULT ''", "DEFAULT NOW()",
      "WITH TIME ZONE"
    ].each do |constraint_name|
      it("should parse constraint_name") { parser.column_constraint.should parse(constraint_name) }
    end
  end

  describe "comment" do
    it("should parse comment") { parser.comment.should parse("-- PK, references area.id\n") }
    it("should parse comment") { parser.comment.should parse("-- PK, references annotation.id\n") }
  end

  describe "column_def" do
    [
      "id SERIAL",
      "editor INTEGER NOT NULL",
      "text TEXT",
      "changelog VARCHAR(255)",
      "created TIMESTAMP",
      "created TIMESTAMP WITH TIME ZONE",
      "created TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
      "area INTEGER NOT NULL",
      "annotation INTEGER NOT NULL"
    ].each do |column_def|
      it("should parse column_def") { parser.column_def.should parse(column_def) }
    end
  end

  describe "last_column_line" do
    it("should parse annotation") { parser.last_column_line.should parse("annotation INTEGER NOT NULL -- PK, references annotation.id\n") }
  end

  $area_annotation_columns = <<-EOT.gsub(/^\s*/, '')
        area        INTEGER NOT NULL, -- PK, references area.id
        annotation  INTEGER NOT NULL -- PK, references annotation.id
  EOT

  describe "column_defs" do
    it("should parse columns with comments") { parser.column_defs.should parse($area_annotation_columns) }
  end

  $table_area_annotation = <<-EOT.gsub(/^\s*/, '')
    CREATE TABLE area_annotation (
        area        INTEGER NOT NULL, -- PK, references area.id
        annotation  INTEGER NOT NULL -- PK, references annotation.id
    );
  EOT

  $artist_table =  <<-EOT.gsub(/^\s*/, '')
CREATE TABLE artist (
    id                  SERIAL,
    gid                 UUID NOT NULL,
    name                VARCHAR NOT NULL,
    sort_name           VARCHAR NOT NULL,
    begin_date_year     SMALLINT,
    begin_date_month    SMALLINT,
    begin_date_day      SMALLINT,
    end_date_year       SMALLINT,
    end_date_month      SMALLINT,
    end_date_day        SMALLINT,
    type                INTEGER, -- references artist_type.id
    area                INTEGER, -- references area.id
    gender              INTEGER, -- references gender.id
    comment             VARCHAR(255) NOT NULL DEFAULT '',
    edits_pending       INTEGER NOT NULL DEFAULT 0 CHECK (edits_pending >= 0),
    last_updated        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    ended               BOOLEAN NOT NULL DEFAULT FALSE
      CONSTRAINT artist_ended_check CHECK (
        (
          -- If any end date fields are not null, then ended must be true
          (end_date_year IS NOT NULL OR
           end_date_month IS NOT NULL OR
           end_date_day IS NOT NULL) AND
          ended = TRUE
        ) OR (
          -- Otherwise, all end date fields must be null
          (end_date_year IS NULL AND
           end_date_month IS NULL AND
           end_date_day IS NULL)
        )
      ),
    begin_area          INTEGER, -- references area.id
    end_area            INTEGER -- references area.id
);
  EOT

  $tag_relation_table =  <<-EOT.gsub(/^\s*/, '')
CREATE TABLE tag_relation
(
    tag1                INTEGER NOT NULL, -- PK, references tag.id
    tag2                INTEGER NOT NULL, -- PK, references tag.id
    weight              INTEGER NOT NULL,
    last_updated        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    CHECK (tag1 < tag2)
);
  EOT

  $area_alias_table =  <<-EOT.gsub(/^\s*/, '')
CREATE TABLE area_alias (
    primary_for_locale  BOOLEAN NOT NULL DEFAULT false,
    ended               BOOLEAN NOT NULL DEFAULT FALSE
      CHECK (
        (
          -- If any end date fields are not null, then ended must be true
          (end_date_year IS NOT NULL OR
           end_date_month IS NOT NULL OR
           end_date_day IS NOT NULL) AND
          ended = TRUE
        ) OR (
          -- Otherwise, all end date fields must be null
          (end_date_year IS NULL AND
           end_date_month IS NULL AND
           end_date_day IS NULL)
        )
      ),
             CONSTRAINT primary_check
                 CHECK ((locale IS NULL AND primary_for_locale IS FALSE) OR (locale IS NOT NULL)));
  EOT

  $artist_alias_table =  <<-EOT.gsub(/^\s*/, '')
CREATE TABLE artist_alias
(
    id                  SERIAL,
    artist              INTEGER NOT NULL, -- references artist.id
    name                VARCHAR NOT NULL,
    locale              TEXT,
    edits_pending       INTEGER NOT NULL DEFAULT 0 CHECK (edits_pending >= 0),
    last_updated        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    type                INTEGER, -- references artist_alias_type.id
    sort_name           VARCHAR NOT NULL,
    begin_date_year     SMALLINT,
    begin_date_month    SMALLINT,
    begin_date_day      SMALLINT,
    end_date_year       SMALLINT,
    end_date_month      SMALLINT,
    end_date_day        SMALLINT,
    primary_for_locale  BOOLEAN NOT NULL DEFAULT false,
    ended               BOOLEAN NOT NULL DEFAULT FALSE
      CHECK (
        (
          -- If any end date fields are not null, then ended must be true
          (end_date_year IS NOT NULL OR
           end_date_month IS NOT NULL OR
           end_date_day IS NOT NULL) AND
          ended = TRUE
        ) OR (
          -- Otherwise, all end date fields must be null
          (end_date_year IS NULL AND
           end_date_month IS NULL AND
           end_date_day IS NULL)
        )
      ),
    CONSTRAINT primary_check CHECK ((locale IS NULL AND primary_for_locale IS FALSE) OR (locale IS NOT NULL)),
    CONSTRAINT search_hints_are_empty
      CHECK (
        (type <> 3) OR (
          type = 3 AND sort_name = name AND
          begin_date_year IS NULL AND begin_date_month IS NULL AND begin_date_day IS NULL AND
          end_date_year IS NULL AND end_date_month IS NULL AND end_date_day IS NULL AND
          primary_for_locale IS FALSE AND locale IS NULL
        )
      )
);
  EOT

  describe "create_table" do
    it("should parse table with comments") { parser.create_table.should parse($table_area_annotation) }
    it("should parse table with table constraint") { parser.create_table.should parse($artist_table) }
    it("should parse table with table constraint") { parser.create_table.should parse($tag_relation_table) }
    it("should parse table with table constraint") { parser.create_table.should parse($area_alias_table) }
    it("should parse table with table constraint") { parser.create_table.should parse($artist_alias_table) }
  end
end
