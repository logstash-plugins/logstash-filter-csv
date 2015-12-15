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
        expect {input.register}.to raise_error
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
        expect(event["column1"]).to eq("big")
        expect(event["column2"]).to eq("bird")
        expect(event["column3"]).to eq("sesame street")
      end

      it "should not mutate the source field" do
        plugin.filter(event)
        expect(event["message"]).to be_kind_of(String)
      end
    end

    describe "custom separator" do
      let(:doc) { "big,bird;sesame street" }

      let(:config) do
        { "separator" => ";" }
      end
      it "extract all the values" do
        plugin.filter(event)
        expect(event["column1"]).to eq("big,bird")
        expect(event["column2"]).to eq("sesame street")
      end
    end

    describe "quote char" do
      let(:doc) { "big,bird,'sesame street'" }

      let(:config) do
        { "quote_char" => "'"}
      end

      it "extract all the values" do
        plugin.filter(event)
        expect(event["column1"]).to eq("big")
        expect(event["column2"]).to eq("bird")
        expect(event["column3"]).to eq("sesame street")
      end

      context "using the default one" do
        let(:doc) { 'big,bird,"sesame, street"' }
        let(:config) { Hash.new }

        it "extract all the values" do
          plugin.filter(event)
          expect(event["column1"]).to eq("big")
          expect(event["column2"]).to eq("bird")
          expect(event["column3"]).to eq("sesame, street")
        end
      end

      context "using a null" do
        let(:doc) { 'big,bird,"sesame" street' }
        let(:config) do
          { "quote_char" => "\x00" }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event["column1"]).to eq("big")
          expect(event["column2"]).to eq("bird")
          expect(event["column3"]).to eq('"sesame" street')
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
        expect(event["first"]).to eq("big")
        expect(event["last"]).to eq("bird")
        expect(event["address"]).to eq("sesame street")
      end

      context "parse csv without autogeneration of names" do

        let(:doc)    { "val1,val2,val3" }
        let(:config) do
          {  "autogenerate_column_names" => false,
             "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event["custom1"]).to eq("val1")
          expect(event["custom2"]).to eq("val2")
          expect(event["column3"]).to be_falsey
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
          expect(event["custom1"]).to eq("val1")
          expect(event["custom2"]).to be_falsey
          expect(event["custom3"]).to eq("val3")
        end
      end

      context "parse csv with more data than defined" do
        let(:doc)    { "val1,val2,val3" }
        let(:config) do
          { "columns" => ["custom1", "custom2"] }
        end

        it "extract all the values" do
          plugin.filter(event)
          expect(event["custom1"]).to eq("val1")
          expect(event["custom2"]).to eq("val2")
          expect(event["column3"]).to eq("val3")
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
          expect(event["custom1"]).to eq("val1")
          expect(event["custom2"]).to eq("val2")
          expect(event["custom3"]).to eq("val3")
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
        expect(event["data"]["column1"]).to eq("big")
        expect(event["data"]["column2"]).to eq("bird")
        expect(event["data"]["column3"]).to eq("sesame street")
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
          expect(event["data"]["column1"]).to eq("big")
          expect(event["data"]["column2"]).to eq("bird")
          expect(event["data"]["column3"]).to eq("sesame street")
        end
      end
    end

    describe "using field convertion" do

      let(:config) do
        { "convert" => { "column1" => "integer", "column3" => "boolean" } }
      end
      let(:doc)   { "1234,bird,false" }
      let(:event) { LogStash::Event.new("message" => doc) }

      it "get converted values to the expected type" do
        plugin.filter(event)
        expect(event["column1"]).to eq(1234)
        expect(event["column2"]).to eq("bird")
        expect(event["column3"]).to eq(false)
      end

      context "when using column names" do

        let(:config) do
          { "convert" => { "custom1" => "integer", "custom3" => "boolean" },
            "columns" => ["custom1", "custom2", "custom3"] }
        end

        it "get converted values to the expected type" do
          plugin.filter(event)
          expect(event["custom1"]).to eq(1234)
          expect(event["custom2"]).to eq("bird")
          expect(event["custom3"]).to eq(false)
        end
      end
    end

    describe "with header" do
      let(:config) do
        { "contains_header" => "true" }
      end

      context "all columns have a header" do
        let(:event1) { LogStash::Event.new("message" => "header1,header2,header3") }
        let(:event2) { LogStash::Event.new("message" => "val1,val2,val3") }

        it "extract headers and values" do
          plugin.filter(event1)
          expect(event1.cancelled?).to eq(true)

          plugin.filter(event2)
          expect(event2.cancelled?).to eq(false)
          expect(event2["header1"]).to eq("val1")
          expect(event2["header2"]).to eq("val2")
          expect(event2["header3"]).to eq("val3")
        end
      end

      context "not all columns have a header" do
        let(:event1) { LogStash::Event.new("message" => "header1") }
        let(:event2) { LogStash::Event.new("message" => "val1,val2") }

        it "generate missing field name" do
          plugin.filter(event1)

          plugin.filter(event2)
          expect(event2["header1"]).to eq("val1")
          expect(event2["column2"]).to eq("val2")
        end
      end

      context "multiple streams" do
        let(:event1) { LogStash::Event.new("message" => "doc1_h1,doc1_h2", "path" => "doc1") }
        let(:event2) { LogStash::Event.new("message" => "val1,val2",       "path" => "doc1") }
        let(:event3) { LogStash::Event.new("message" => "doc2_h1,doc2_h2", "path" => "doc2") }
        let(:event4) { LogStash::Event.new("message" => "val3,val4",       "path" => "doc2") }
        let(:event5) { LogStash::Event.new("message" => "val5,val6",       "path" => "doc1") }

        it "extract headers and values" do
          plugin.filter(event1)
          expect(event1.cancelled?).to eq(true)

          plugin.filter(event2)
          expect(event2["doc1_h1"]).to eq("val1")
          expect(event2["doc1_h2"]).to eq("val2")

          plugin.filter(event3)
          expect(event3.cancelled?).to eq(true)

          plugin.filter(event4)
          expect(event4["doc2_h1"]).to eq("val3")
          expect(event4["doc2_h2"]).to eq("val4")

          plugin.filter(event5)
          expect(event5["doc1_h1"]).to eq("val5")
          expect(event5["doc1_h2"]).to eq("val6")
        end
      end

      context "given identity field" do
        let(:config) do
          { "contains_header" => "true",
            "stream_identity" => "%{identity}" }
        end

        let(:event1) { LogStash::Event.new("message" => "doc1_h1,doc1_h2", "identity" => "doc1") }
        let(:event2) { LogStash::Event.new("message" => "doc2_h1,doc2_h2", "identity" => "doc2") }
        let(:event3) { LogStash::Event.new("message" => "val1,val2",       "identity" => "doc1") }
        let(:event4) { LogStash::Event.new("message" => "val3,val4",       "identity" => "doc2") }

        it "extract headers and values" do
          plugin.filter(event1)
          expect(event1.cancelled?).to eq(true)

          plugin.filter(event2)
          expect(event2.cancelled?).to eq(true)

          plugin.filter(event3)
          expect(event3["doc1_h1"]).to eq("val1")
          expect(event3["doc1_h2"]).to eq("val2")

          plugin.filter(event4)
          expect(event4["doc2_h1"]).to eq("val3")
          expect(event4["doc2_h2"]).to eq("val4")
        end
      end
    end
  end
end
