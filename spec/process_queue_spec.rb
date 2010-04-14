require File.join(File.dirname(__FILE__), '..', 'lib','process_queue')

#------------------------------------------------------------------------------#
describe ProcessQueue, "with default construction" do
  before(:all) do
    @queue = ProcessQueue.new
  end

  it "should default :database to nil" do
    @queue.database.should == nil
  end
  it "should default :host to nil" do
    @queue.host.should == nil
  end
  it "should default :username to nil" do
    @queue.username.should == nil
  end
  it "should default :password to nil" do
    @queue.password.should == nil
  end
  it "should default :namespace to nil" do
    @queue.namespace.should == nil
  end
  it "should default dbh to nil" do
    @queue.dbh.should == nil
  end
end

describe ProcessQueue, "with specified construction" do
  before(:all) do
    @database  = ""
    @host      = ""
    @username  = ""
    @password  = ""
    @namespace = ""
    @content   = { "foo" => "bar" }

    @queue = ProcessQueue.new(:database => @database, :host => @host, :username => @username, :password => @password, :namespace => @namespace)
  end

  #----- test attributes -----#
  it "should default :database to ''" do
    @queue.database.should == ""
  end
  it "should default :host to ''" do
    @queue.host.should == ""
  end
  it "should default :username to ''" do
    @queue.username.should == ''
  end
  it "should default :password to ''" do
    @queue.password.should == ''
  end
  it "should default :namespace to ''" do
    @queue.namespace.should == ''
  end

  #----- testing methods -----#
  it "should return true if it can connect" do
    @queue.validate_connection.should == true
    @queue.dbh.should_not == nil
  end

  it "should push content onto the queue" do
    @queue.push(@content).should == true

    #----- run query to make sure the content is correct -----#
    sql = <<SQL
SELECT id, payload, status 
  FROM process_queue 
 ORDER BY id DESC 
 LIMIT 1
SQL
    row = @queue.dbh.query(sql).fetch_row
    row[1].should == '{"foo":"bar"}'
    row[2].should == 'new'

    @queue.dbh.query("DELETE FROM process_queue WHERE id = #{row[0]}")
    @queue.dbh.commit
  end

  it "should pop content off of the queue" do
    @queue.push(@content).should == true
    @queue.pop.should == @content

    sql = "SELECT status FROM process_queue WHERE id = #{@queue.active_id}"
    row = @queue.dbh.query(sql).fetch_row
    row[0].should == 'open'

    @queue.dbh.query("DELETE FROM process_queue WHERE id = #{@queue.active_id}")
    @queue.dbh.commit
  end

  it "should return a payload and delete the record on pop!" do
    @queue.push(@content).should == true
    @queue.pop!.should == @content

    sql = "SELECT * FROM process_queue WHERE id = #{@queue.active_id}"
    r = @queue.dbh.query(sql).fetch_row

    r.should == nil
  end

  it "should do something intelligent if there is nothing in the queue" do
    @queue.pop.should == false
  end

  it "should block other reads to the same record" do
  end

end
