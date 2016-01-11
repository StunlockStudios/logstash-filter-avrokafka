# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

class LogStash::Filters::Avrokafka < LogStash::Filters::Base

  config_name "avrokafka"
  
  # Replace the message with this value.
  config :message, :validate => :string, :default => "Hello World!"
  

  public
  def register
    # Add instance variables 
  end # def register

  public
  def filter(event)
    if @message
      event["message"] = @message
    end
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::Avrokafka
