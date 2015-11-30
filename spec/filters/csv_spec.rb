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

  describe "parse csv with more data than defined column names without autogeneration of column names" do
    config <<-CONFIG
      filter {
        csv {
          autogenerate_column_names => false
          columns => ["custom1", "custom2"]
        }
      }
    CONFIG

    sample "val1,val2,val3" do
      insist { subject["custom1"] } == "val1"
      insist { subject["custom2"] } == "val2"
      reject { subject.include?("column3") }
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

  describe "parse csv from a given source with column names while dropping empty columns" do
    config <<-CONFIG
      filter {
        csv {
          skip_empty_columns => true
          source => "datafield"
          columns => ["custom1", "custom2", "custom3"]
        }
      }
    CONFIG

    sample("datafield" => "val1,,val3") do
      insist { subject["custom1"] } == "val1"
      reject { subject.include?("custom2") }
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

  # Use a more rspec3 syntax, avoiding the usage of sample
  # and insist in favor of rspec helpers methods
  describe LogStash::Filters::CSV  do


    subject(:plugin) { LogStash::Filters::CSV.new(config) }
    let(:config) { Hash.new }

    let(:doc) { ""}
    let(:event) { LogStash::Event.new("message" => doc) }


    before(:each) do
      plugin.register
    end

    describe "using default values" do
      let(:doc) { "big,bird,sesame street" }
      it "should not mutate the source field" do
        plugin.filter(event)
        expect(event["message"]).to be_kind_of(String)
      end
    end

  end


end
