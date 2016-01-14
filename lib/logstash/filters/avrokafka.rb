# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require 'net/http'
require "open-uri"
require "json"
require "avro"

# Confusing name, does not actually have anything to do with Kafka. Takes a raw binary blob of serialized
# avro schema/data(?) with the avro schema id only at the top of the blob, after a magic byte identifier.
# Will/should be redone as a codec used with the kafka input plugin instead. Will be very similar to the 
# existing avro codec, just differing in that the schema id is defined in the beginning of the blob and
# fetched from the configured schema_registry url instead of being one "hard configured" schema.
#
# Byte 0, Size 1: Magic Byte Identifier
# Byte 1, Size 4: Avro Schema ID
# Byte 5, Size -: Serialized Avro data.
class LogStash::Filters::Avrokafka < LogStash::Filters::Base

  config_name "avrokafka"

  # URL of where to fetch the Avro schema. The schema id will be appended directly at
  # the end of the URL so make sure to get it right with ending slashes et cetera.  
  config :schema_registry, :validate => :string, :required => true

  # A byte identifier at the top/beginning of the blob.
  config :magic_byte, :validate => :number, :default => 255, :required => false

  # Size in bytes of the schema id. Up to four (4) bytes.
  config :schema_id_size, :validate => :number, :default => 4, :required => false

  public
  def register
    @schema_list = { }
  end # def register

  public
  def filter(event)

    data = event["message"]
    magic = data.getbyte(0)

    if magic == @magic_byte

      @schema_id_size = 4 if @schema_id_size < 0 || @schema_id_size > 4

      # A bit messy. Maybe just support schema_id as a four (4) byte integer.
      schema_id = -1
      schema_id = data.getbyte(1)                     if @schema_id_size > 0
      schema_id = schema_id | (data.getbyte(2) << 8)  if @schema_id_size > 1
      schema_id = schema_id | (data.getbyte(3) << 16) if @schema_id_size > 2
      schema_id = schema_id | (data.getbyte(4) << 24) if @schema_id_size > 3


      avro_data = data[5..-1]
      avro_schema = get_schema(schema_id)

      if avro_schema == nil
        # TODO: Log?
        event.cancel
        return
      end

      datum = StringIO.new(avro_data)
      decoder = Avro::IO::BinaryDecoder.new(datum)
      datum_reader = Avro::IO::DatumReader.new(avro_schema)
      parsed = datum_reader.read(decoder)

      event["message"] = nil
      event["avro_schema_id"] = schema_id
      event["schema_registry"] = @schema_registry

      # Put all parsed (root) stuff in the root of event.
      parsed.each{|k, v| event[k] = v}

      # TODO: Need to do this outside of a filter/codec like this. Ugly hack for now.
      # Fixed now, but still here for clients not yet updated.
      if !event["unixtime"]
        event["unixtime"] = (event["time"].to_i - 621355968000000000) / 10000
      end
    else
      # TODO: Log?
      event.cancel
    end

    filter_matched(event)
  end # def filter

  private
  def get_schema(id)
    if @schema_list.key?(id)
      return @schema_list[id]
    else
      schema_file = download_schema_file(id)
      if schema_file != nil && schema_file != ""
        json = JSON.parse(schema_file)
        if json != nil && json.key?("schema")
          avro_schema = Avro::Schema.parse(json["schema"].to_s)
          @schema_list[id] = avro_schema
          return avro_schema
        end
      end
    end
    return nil
  end
  
  private
  def download_schema_file(id)
    uri = URI.parse("#{@schema_registry}#{id.to_s}")
    http_object = Net::HTTP.new(uri.host, uri.port)
    http_object.use_ssl = true if uri.scheme == 'https'
    begin
      http_object.start do |http|
        request = Net::HTTP::Get.new uri.request_uri
        http.read_timeout = 30
        http.request request do |response|
          schema = ""
          response.read_body do |chunk|
            schema += chunk
          end
          #json = JSON.parse(schema)
          #return json["schema"].to_s;
          return schema;
        end
      end
    rescue Exception => e
      #puts "=> Exception: '#{e}'. Skipping download."
    end
    return nil
  end

end # class LogStash::Filters::Avrokafka
