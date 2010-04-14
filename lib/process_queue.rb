require 'mysql'
require 'json/pure'

class ProcessQueue
  attr_accessor :dbh, :database, :host, :username, :password, :payload, 
                :namespace
  attr_reader :active_id

  VERSION = '0.0.1'

  DEFAULT_OPTIONS = {
    :dbh       => nil,
    :database  => nil,
    :host      => nil,
    :username  => nil,
    :password  => nil,
    :namespace => nil
  }

  #----------------------------------------------------------------------------#
  def initialize(*args)
    opts = {}

    case
    when args.length == 0 then
    when args.length == 1 && args[0].class == Hash then
      arg = args.shift

      if arg.class == Hash
        opts = arg
      end
    else
      raise ArgumentError, "new() expects hash or hashref as argument"
    end

    opts = DEFAULT_OPTIONS.merge opts

    @dbh       = opts[:dbh]
    @database  = opts[:database]
    @host      = opts[:host]
    @username  = opts[:username]
    @password  = opts[:password]
    @namespace = opts[:namespace]

    @active_id = nil
  end

  #----------------------------------------------------------------------------#
  def validate_connection
    connected = false
    counter = 1 

    #----- loop until we are reconnected -----#
    until connected do
      begin
        db_connect
        connected = true
      rescue
        sleep(2 ** counter)
        counter += 1 unless counter >= 8
      end
    end
    connected
  end

  #----------------------------------------------------------------------------#
  def push(payload)
    success = false
    namespace = @dbh.quote(@namespace)
    @payload = @dbh.quote(JSON.generate(payload))

    sql = <<SQL
INSERT INTO process_queue (namespace, payload) 
VALUES ('#{namespace}','#{@payload}')
SQL

    begin
      @dbh.query(sql)
      @active_id = @dbh.insert_id
      success = true
    rescue
      success = false
    end

    return success
  end

  #----------------------------------------------------------------------------#
  def pop
    namespace_constraint = @namespace ? "   AND namespace = '#{@dbh.quote(@namespace)}'" : ''

    sql = <<SQL
SELECT id, payload 
  FROM process_queue 
 WHERE status = 'new'
#{namespace_constraint}
 LIMIT 1
   FOR UPDATE
SQL
    @dbh.query("START TRANSACTION")
    record = @dbh.query(sql).fetch_row

    return false unless record

    @active_id = record[0]

    update_status('open')
    @dbh.commit

    @payload = JSON.parse(record[1])
  end

  #----------------------------------------------------------------------------#
  def pop!
    self.pop

    sql = "DELETE FROM process_queue WHERE id = #{@active_id}"

    begin
      @dbh.query(sql)
    rescue
      return false
    end

    @payload
  end

  #----------------------------------------------------------------------------#
  def close
    update_status('closed')
  end

  #----------------------------------------------------------------------------#
  def error
    update_status('error')
  end

  #----------------------------------------------------------------------------#
  def disconnect
    @dbh.close
  end
  
  private

  #----------------------------------------------------------------------------#
  def db_connect
    begin
      @dbh.ping
    rescue
      @dbh = Mysql::new(@host, @username, @password, @database)
    end
  end

  #----------------------------------------------------------------------------#
  def update_status(status)
    sql = <<SQL
UPDATE process_queue 
   SET status = '#{status}' 
 WHERE id = #{@active_id}
SQL

    begin
      @dbh.query(sql)
    rescue => e
      return false
    end
    return @dbh.affected_rows >= 1
  end
end
