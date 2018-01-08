# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/filters/csv"

describe LogStash::Filters::CSV do

  subject(:plugin) { LogStash::Filters::CSV.new(config) }
  let(:config) { Hash.new }

  let(:doc)   { "" }
  let(:event) { LogStash::Event.new("message" => doc) }

  describe "registration" do

    context "when using invalid data types" do
      let(:config) do
        { "convert" => { "custom1" => "integer", "custom3" => "wrong_type" },
          "columns" => ["custom1", "custom2", "custom3"] }
      end

      it "should register" do
        input = LogStash::Plugin.lookup("filter", "csv").new(config)
        expect {input.register}.to raise_error(LogStash::ConfigurationError)
      end
    end
  end

  describe "receive" do

    before(:each) do
      plugin.register
    end

    describe "all defaults" do

      let(:config) { Hash.new }

      let(:doc) { "big,bird,sesame street" }

      it "extract all the values" do
        plugin.filter(event)
        expect(event.get("column1")).to eq("big")
        expect(event.get("column2")).to eq("bird")
        expect(event.get("column3")).to eq("sesame street")
      end

      it "should not mutate the source field" do
        plugin.filter(event)
        expect(event.get("message")).to be_kind_of(String)
      end
    end

    describe "empty message" do
      let(:doc) { "" }

      let(:config) do
        { "skip_empty_rows" => true }
      end

      it "skips empty rows" do
        plugin.filter(event)
        expect(event.get("tags")).to include("_csvskippedemptyfield") 
        expect(event).not_to be_cancelled 
      end
    end

    describe "custom separator" do
      let(:doc) { "big,bird;sesame street" }

      let(:config) do
        { "separator" => ";" }
      end
      it "extract all the values" do
        plugin.filter(event)
        expect(event.get("column1")).to eq("big,bird")
        expect(event.get("column2")).to eq("sesame street")
      end
    end

    describe "quote char" do
      let(:doc) { "big,bird,'sesame street'" }

      let(:config) do
        { "quote_char" => "'"}
      end

      it "extract all the values" do
        plugin.filter(event)
        expect(event.get("column1")).to eq("big")
        expect(event.get("column2")).to eq("bird")
        expect(event.get("column3")).to eq("sesame street")
      end

      context "using the default one" do
        let(:doc) { 'big,bird,"sesame, street"' }
        let(:config) { Hash.new }

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("column1")).to eq("big")
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq("sesame, street")
        end
      end

      context "using a null" do
        let(:doc) { 'big,bird,"sesame" street' }
        let(:config) do
          { "quote_char" => "\x00" }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("column1")).to eq("big")
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq('"sesame" street')
        end
      end
      
      context "using a null as read from config" do
        let(:doc) { 'big,bird,"sesame" street' }
        let(:config) do
          { "quote_char" => "\\x00" }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("column1")).to eq("big")
          expect(event.get("column2")).to eq("bird")
          expect(event.get("column3")).to eq('"sesame" street')
        end
      end
    end

    describe "given column names" do
      let(:doc)    { "big,bird,sesame street" }
      let(:config) do
        { "columns" => ["first", "last", "address" ] }
      end

      it "extract all the values" do
        plugin.filter(event)
        expect(event.get("first")).to eq("big")
        expect(event.get("last")).to eq("bird")
        expect(event.get("address")).to eq("sesame street")
      end

      context "parse csv without autogeneration of names" do

        let(:doc)    { "val1,val2,val3" }
        let(:config) do
          {  "autogenerate_column_names" => false,
             "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("custom1")).to eq("val1")
          expect(event.get("custom2")).to eq("val2")
          expect(event.get("column3")).to be_falsey
        end
      end

      context "parse csv and skip the header" do
        let(:doc)    { "first_column,second_column,third_column" }
        let(:config) do
          { "skip_header" => true,
            "columns" => ["first_column", "second_column", "third_column"] }
        end

        it "expects the event to be cancelled" do
          plugin.filter(event)
          expect(event).to be_cancelled
        end
      end

      context "parse csv skipping empty columns" do

        let(:doc)    { "val1,,val3" }

        let(:config) do
          { "skip_empty_columns" => true,
            "source" => "datafield",
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        let(:event) { LogStash::Event.new("datafield" => doc) }

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("custom1")).to eq("val1")
          expect(event.get("custom2")).to be_falsey
          expect(event.get("custom3")).to eq("val3")
        end
      end

      context "parse csv with more data than defined" do
        let(:doc)    { "val1,val2,val3" }
        let(:config) do
          { "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("custom1")).to eq("val1")
          expect(event.get("custom2")).to eq("val2")
          expect(event.get("column3")).to eq("val3")
        end
      end

      context "parse csv from a given source" do
        let(:doc)    { "val1,val2,val3" }
        let(:config) do
          { "source"  => "datafield",
            "columns" => ["custom1", "custom2", "custom3"] }
        end
        let(:event) { LogStash::Event.new("datafield" => doc) }

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("custom1")).to eq("val1")
          expect(event.get("custom2")).to eq("val2")
          expect(event.get("custom3")).to eq("val3")
        end
      end

      context "that use [@metadata]" do
        let(:metadata_field) { "[@metadata][one]" }
        let(:config) do
          {
            "columns" => [ metadata_field, "foo" ]
          }
        end

        let(:event) { LogStash::Event.new("message" => "hello,world") }

        before do
          plugin.filter(event)
        end

        it "should work correctly" do
          expect(event.get(metadata_field)).to eq("hello")
        end
      end
    end

    describe "givin target" do
      let(:config) do
        { "target" => "data" }
      end
      let(:doc)   { "big,bird,sesame street" }
      let(:event) { LogStash::Event.new("message" => doc) }

      it "extract all the values" do
        plugin.filter(event)
        expect(event.get("data")["column1"]).to eq("big")
        expect(event.get("data")["column2"]).to eq("bird")
        expect(event.get("data")["column3"]).to eq("sesame street")
      end

      context "when having also source" do
        let(:config) do
          {  "source" => "datain",
             "target" => "data" }
        end
        let(:event) { LogStash::Event.new("datain" => doc) }
        let(:doc)   { "big,bird,sesame street" }

        it "extract all the values" do
          plugin.filter(event)
          expect(event.get("data")["column1"]).to eq("big")
          expect(event.get("data")["column2"]).to eq("bird")
          expect(event.get("data")["column3"]).to eq("sesame street")
        end
      end

      context "which uses [nested][fieldref] syntax" do
        let(:target) { "[foo][bar]" }
        let(:config) do
          {
            "target" => target
          }
        end

        let(:event) { LogStash::Event.new("message" => "hello,world") }

        before do
          plugin.filter(event)
        end

        it "should set fields correctly in the target" do
          expect(event.get("#{target}[column1]")).to eq("hello")
          expect(event.get("#{target}[column2]")).to eq("world")
        end

        context "with nested fieldrefs as columns" do
          let(:config) do
            {
              "target" => target,
              "columns" => [ "[test][one]", "[test][two]" ]
            }
          end

          it "should set fields correctly in the target" do
            expect(event.get("#{target}[test][one]")).to eq("hello")
            expect(event.get("#{target}[test][two]")).to eq("world")
          end
        end

      end
    end

    describe "using field convertion" do

      let(:config) do
        {
            "convert" => {
                "column1" => "integer",
                "column3" => "boolean",
                "column4" => "float",
                "column5" => "date",
                "column6" => "date_time",
                "column7" => "date",
                "column8" => "date_time",
            }
        }
      end
      # 2017-06-01,2001-02-03T04:05:06+07:00
      let(:doc)   { "1234,bird,false,3.14159265359,2017-06-01,2001-02-03 04:05:06,invalid_date,invalid_date_time" }
      let(:event) { LogStash::Event.new("message" => doc) }

      it "converts to integer" do
        plugin.filter(event)
        expect(event.get("column1")).to eq(1234)
      end

      it "does not convert without converter" do
        plugin.filter(event)
        expect(event.get("column2")).to eq("bird")
      end

      it "converts to boolean" do
        plugin.filter(event)
        expect(event.get("column3")).to eq(false)
      end

      it "converts to float" do
        plugin.filter(event)
        expect(event.get("column4")).to eq(3.14159265359)
      end

      it "converts to date" do
        plugin.filter(event)
        expect(event.get("column5")).to be_a(LogStash::Timestamp)
        expect(event.get("column5").to_s).to eq(LogStash::Timestamp.new(Date.parse("2017-06-01").to_time).to_s)
      end

      it "converts to date_time" do
        plugin.filter(event)
        expect(event.get("column6")).to be_a(LogStash::Timestamp)
        expect(event.get("column6").to_s).to eq(LogStash::Timestamp.new(DateTime.parse("2001-02-03 04:05:06").to_time).to_s)
      end

      it "tries to converts to date but return original" do
        plugin.filter(event)
        expect(event.get("column7")).to eq("invalid_date")
      end

      it "tries to converts to date_time but return original" do
        plugin.filter(event)
        expect(event.get("column8")).to eq("invalid_date_time")
      end

      context "when using column names" do

        let(:config) do
          { "convert" => { "custom1" => "integer", "custom3" => "boolean" },
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "get converted values to the expected type" do
          plugin.filter(event)
          expect(event.get("custom1")).to eq(1234)
          expect(event.get("custom2")).to eq("bird")
          expect(event.get("custom3")).to eq(false)
        end
      end
    end

    describe "given autodetect option" do
      let(:header) { LogStash::Event.new("message" => "first,last,address") }
      let(:doc)    { "big,bird,sesame street" }
      let(:config) do
        { "autodetect_column_names" => true }
      end

      it "extract all the values with the autodetected header" do
        plugin.filter(header)
        plugin.filter(event)
        expect(event.get("first")).to eq("big")
        expect(event.get("last")).to eq("bird")
        expect(event.get("address")).to eq("sesame street")
      end
    end
  end
end
