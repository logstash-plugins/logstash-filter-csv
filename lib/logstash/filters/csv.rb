# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

require "csv"

# The CSV filter takes an event field containing CSV data, parses it,
# and stores it as individual fields (can optionally specify the names).
# This filter can also parse data with any separator, not just commas.
class LogStash::Filters::CSV < LogStash::Filters::Base
  config_name "csv"

  # The CSV data in the value of the `source` field will be expanded into a
  # data structure.
  config :source, :validate => :string, :default => "message"

  # Define a list of column names (in the order they appear in the CSV,
  # as if it were a header line). If `columns` is not configured, or there
  # are not enough columns specified, the default column names are
  # "column1", "column2", etc. In the case that there are more columns
  # in the data than specified in this column list, extra columns will be auto-numbered:
  # (e.g. "user_defined_1", "user_defined_2", "column3", "column4", etc.)
  config :columns, :validate => :array, :default => []

  # Define if CSV data contains header line. If `contains_header` is `true`,
  # first line will be parsed as column names. In the case that there are more columns
  # in the data than parsed from the header line, specified `columns` will be used.
  # In the case `columns` are not specified or data contain more columns than
  # there are `columns` specified, auto-numbered columns will be used.
  config :contains_header, :validate => :boolean, :default => false

  # If `contains_header` is `true`, this is how the filter determines which
  # stream an event belongs to. The header will be read for every identified stream,
  # e.g. for every input file it will use the first line as a header line.
  config :stream_identity, :validate => :string, :default => "%{host}.%{path}.%{type}"

  # Define the column separator value. If this is not specified, the default
  # is a comma `,`.
  # Optional.
  config :separator, :validate => :string, :default => ","

  # Define the character used to quote CSV fields. If this is not specified
  # the default is a double quote `"`.
  # Optional.
  config :quote_char, :validate => :string, :default => '"'

  # Define target field for placing the data.
  # Defaults to writing to the root of the event.
  config :target, :validate => :string

  public
  def register
    @headers = {}
  end # def register

  public
  def filter(event)
    return unless filter?(event)
    @logger.debug("Running csv filter", :event => event)

    matches = 0

    if event[@source]
      if event[@source].is_a?(String)
        event[@source] = [event[@source]]
      end

      if event[@source].length > 1
        @logger.warn("csv filter only works on fields of length 1",
                     :source => @source, :value => event[@source],
                     :event => event)
        return
      end

      raw = event[@source].first
      begin
        stream = event.sprintf(@stream_identity)
        if @contains_header && !@headers.key?(stream)
          @headers[stream] = parse_line(raw)
          event.cancel
        else
          values = parse_line(raw)

          if @target.nil?
            # Default is to write to the root of the event.
            dest = event
          else
            dest = event[@target] ||= {}
          end

          values.each_index do |i|
            field_name = header(stream, i)
            dest[field_name] = values[i]
          end

          filter_matched(event)
        end

      rescue => e
        event.tag "_csvparsefailure"
        @logger.warn("Trouble parsing csv", :source => @source, :raw => raw,
                      :exception => e)
        return
      end # begin
    end # if event

    @logger.debug("Event after csv filter", :event => event)

  end # def filter

  private
  def parse_line(line)
    CSV.parse_line(line, :col_sep => @separator, :quote_char => @quote_char)
  end

  private
  def header(stream, column)
    user_defined = if @headers.key?(stream)
                     @headers[stream][column]
                   else
                     @columns[column]
                   end
    user_defined || "column#{column+1}"
  end

end # class LogStash::Filters::Csv

