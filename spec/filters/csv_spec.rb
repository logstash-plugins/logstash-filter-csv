# encoding: utf-8

require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/csv"

describe LogStash::Filters::CSV do

  describe "all defaults" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv { }
      }
    CONFIG

    sample "big,bird,sesame street" do
      insist { subject["column1"] } == "big"
      insist { subject["column2"] } == "bird"
      insist { subject["column3"] } == "sesame street"
    end
  end

  describe "custom separator" do
    config <<-CONFIG
      filter {
        csv {
          separator => ";"
        }
      }
    CONFIG

    sample "big,bird;sesame street" do
      insist { subject["column1"] } == "big,bird"
      insist { subject["column2"] } == "sesame street"
    end
  end

  describe "custom quote char" do
    config <<-CONFIG
      filter {
        csv {
          quote_char => "'"
        }
      }
    CONFIG

    sample "big,bird,'sesame street'" do
      insist { subject["column1"] } == "big"
      insist { subject["column2"] } == "bird"
      insist { subject["column3"] } == "sesame street"
    end
  end

  describe "default quote char" do
    config <<-CONFIG
      filter {
        csv {
        }
      }
    CONFIG

    sample 'big,bird,"sesame, street"' do
      insist { subject["column1"] } == "big"
      insist { subject["column2"] } == "bird"
      insist { subject["column3"] } == "sesame, street"
    end
  end
  describe "null quote char" do
    config <<-CONFIG
      filter {
        csv {
          quote_char => "\x00"
        }
      }
    CONFIG

    sample 'big,bird,"sesame" street' do
      insist { subject["column1"] } == 'big'
      insist { subject["column2"] } == 'bird'
      insist { subject["column3"] } == '"sesame" street'
    end
  end

  describe "given columns" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          columns => ["first", "last", "address" ]
        }
      }
    CONFIG

    sample "big,bird,sesame street" do
      insist { subject["first"] } == "big"
      insist { subject["last"] } == "bird"
      insist { subject["address"] } == "sesame street"
    end
  end

  describe "parse csv with more data than defined column names" do
    config <<-CONFIG
      filter {
        csv {
          columns => ["custom1", "custom2"]
        }
      }
    CONFIG

    sample "val1,val2,val3" do
      insist { subject["custom1"] } == "val1"
      insist { subject["custom2"] } == "val2"
      insist { subject["column3"] } == "val3"
    end
  end


  describe "parse csv from a given source with column names" do
    config <<-CONFIG
      filter {
        csv {
          source => "datafield"
          columns => ["custom1", "custom2", "custom3"]
        }
      }
    CONFIG

    sample("datafield" => "val1,val2,val3") do
      insist { subject["custom1"] } == "val1"
      insist { subject["custom2"] } == "val2"
      insist { subject["custom3"] } == "val3"
    end
  end

  describe "given target" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          target => "data"
        }
      }
    CONFIG

    sample "big,bird,sesame street" do
      insist { subject["data"]["column1"] } == "big"
      insist { subject["data"]["column2"] } == "bird"
      insist { subject["data"]["column3"] } == "sesame street"
    end
  end

  describe "given target and source" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          source => "datain"
          target => "data"
        }
      }
    CONFIG

    sample("datain" => "big,bird,sesame street") do
      insist { subject["data"]["column1"] } == "big"
      insist { subject["data"]["column2"] } == "bird"
      insist { subject["data"]["column3"] } == "sesame street"
    end
  end

  describe "with header" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          contains_header => true
        }
      }
    CONFIG

    sample [ "header1,header2,header3", "val1,val2,val3" ] do
      insist { subject["header1"] } == "val1"
      insist { subject["header2"] } == "val2"
      insist { subject["header3"] } == "val3"
    end
  end

  describe "more columns in data than in header" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          contains_header => true
        }
      }
    CONFIG

    sample [ "header1", "val1,val2" ] do
      insist { subject["header1"] } == "val1"
      insist { subject["column2"] } == "val2"
    end
  end

  describe "multiple streams" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          contains_header => true
        }
      }
    CONFIG

    # We mix two CSV files with different paths
    eventstream = [
        LogStash::Event.new("message" => "doc1_h1,doc1_h2", "path" => "doc1"),
        LogStash::Event.new("message" => "val1,val2",       "path" => "doc1"),
        LogStash::Event.new("message" => "doc2_h1,doc2_h2", "path" => "doc2"),
        LogStash::Event.new("message" => "val3,val4",       "path" => "doc2"),
        LogStash::Event.new("message" => "val5,val6",       "path" => "doc1"),
    ]

    events = eventstream.map{|event| event.to_hash}

    sample events do
      expect(subject).to be_a(Array)
      insist { subject.size } == 3

      # Make sure headers from both CSV files are used
      insist { subject[0]["doc1_h1"] } == "val1"
      insist { subject[0]["doc1_h2"] } == "val2"
      insist { subject[1]["doc2_h1"] } == "val3"
      insist { subject[1]["doc2_h2"] } == "val4"
      insist { subject[2]["doc1_h1"] } == "val5"
      insist { subject[2]["doc1_h2"] } == "val6"
    end
  end

  describe "given identity field" do
    # The logstash config goes here.
    # At this time, only filters are supported.
    config <<-CONFIG
      filter {
        csv {
          contains_header => true
          stream_identity => "%{identity}"
        }
      }
    CONFIG

    eventstream = [
        LogStash::Event.new("message" => "doc1_h1,doc1_h2", "identity" => "doc1"),
        LogStash::Event.new("message" => "doc2_h1,doc2_h2", "identity" => "doc2"),
        LogStash::Event.new("message" => "val1,val2",       "identity" => "doc1"),
        LogStash::Event.new("message" => "val3,val4",       "identity" => "doc2"),
    ]

    events = eventstream.map{|event| event.to_hash}

    sample events do
      expect(subject).to be_a(Array)
      insist { subject.size } == 2

      insist { subject[0]["doc1_h1"] } == "val1"
      insist { subject[0]["doc1_h2"] } == "val2"
      insist { subject[1]["doc2_h1"] } == "val3"
      insist { subject[1]["doc2_h2"] } == "val4"
    end
  end
end
