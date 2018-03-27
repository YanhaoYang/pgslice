require_relative "../connection"
require_relative "../strategies"

module PgSlice
  module Commands
    class Base
      attr_reader :options, :conn, :strategy

      def initialize(options = {})
        @options = options
        @conn = Connection.new(options[:url])
        @strategy = load_strategy(options[:strategy]).new(@conn, options)
      end

      def run
        raise "Not implemented"
      end

      private

      def load_strategy(name_and_path)
        name, path = name_and_path.split('@')
        load path if path
        Module.const_get name
      end
    end
  end
end
