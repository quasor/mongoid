# encoding: utf-8
module Mongoid #:nodoc:
  module Collections #:nodoc:
    class Master

      attr_reader :collection

      # All read and write operations should delegate to the master connection.
      # These operations mimic the methods on a Mongo:Collection.
      #
      # Example:
      #
      # <tt>collection.save({ :name => "Al" })</tt>
      Operations::ALL.each do |name|
        define_method(name) do |*args| 
          rescue_connection_failure do
            collection.send(name, *args) 
          end
        end
      end

      # Create the new database writer. Will create a collection from the
      # master database.
      #
      # Example:
      #
      # <tt>Master.new(master, "mongoid_people")</tt>
      def initialize(master, name)
        @collection = master.collection(name)
      end
      
      # Ensure retry upon failure
      def rescue_connection_failure(max_retries=10)
        retries = 0
        begin
          yield
        rescue Mongo::ConnectionFailure => ex
          retries += 1
          raise ex if retries >= max_retries
          sleep(0.5)
          retry
        end
      end

      
    end
  end
end
