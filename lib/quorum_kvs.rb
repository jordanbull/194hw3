require 'rubygems'
require 'bud'
require 'kvs/kvs'
require 'kvs/quorum_kvsproto'
require 'membership/membership'

module QuorumKVS
  include StaticMembership
  include QuorumKVSProtocol
  import BasicKVS => :kvs
  
  @@r_num = -1.0
  @@w_num = -1.0

  state do
  	table :r_w, [] => [:r_fraction, :w_fraction]
  	channel :kvput_chan, [:@dest, :from, :t] + kvput.key_cols => kvput.val_cols
    channel :kvget_chan, [:@dest, :from, :t] + kvget.key_cols => kvget.val_cols
    channel :kvget_response_chan, [:@dest, :t] + kvget_response.key_cols => kvget_response.val_cols
    channel :kv_acks_chan, [:@dest, :reqid, :t]
    table :buffer, [:reqid] => [:request]

    table :current_request, buffer.schema
    table :waiting, buffer.schema
    table :get_count, [:reqid, :time, :key, :oldtime, :value]
    table :put_count, [:reqid, :time]

    scratch :cur_put, kvput.schema
    scratch :cur_put2, cur_put.schema
    scratch :cur_get, kvget.schema

    lmax :time
  end

  bloom :config do
  	# sets the quorum_configs if they aren't already set
  	r_w <= quorum_config
  end

  bloom :buffering do
  	buffer <= kvput {|p| [p.reqid, [p]]}
  	buffer <= kvget {|g| [g.reqid, [g]]}
    current_request <= buffer.argmin([], :reqid)
    #current_request <= buffer.argmin([], :reqid) do |b|
    #	b if waiting.to_a.length == 0
    #end
    buffer <- current_request
  end

  
  # requests are re-routed to "chosen" destination(s)
  bloom :requests do
  	cur_put <= current_request{|c| c.request.to_a[0].to_a if c.request.to_a[0].to_a.length == 4}
    cur_put2 <= cur_put{|c| [c.to_a[0], c.to_a[1], c.to_a[2], [time.reveal, c.to_a[3]]]} #makes the value stored [budtime, value] for most recent comparisons
  	cur_get <= current_request{|c| c.request.to_a[0].to_a if c.request.to_a[0].to_a.length == 2}
  	waiting <= current_request
  	current_request <- current_request
    kvput_chan <~ (member * cur_put2).pairs{|m,k| [m.host, ip_port, time.reveal] + k.to_a}
    kvget_chan <~ (member * cur_get).pairs{|m,k| [m.host, ip_port, time.reveal] + k.to_a}
  end

  # receiver-side logic for re-routed requests
  bloom :receive_requests do
    kvs.kvput <= kvput_chan{|k| k.to_a.drop(3) }
    kvs.kvget <= kvget_chan{|k| k.to_a.drop(3) }
    kv_acks_chan <~ kvput_chan{|k| [k.from, k.reqid, time.reveal] }
    kvget_response_chan <~ (kvget_chan * kvs.kvget_response).outer(:reqid => :reqid) do |c, r|
      [c.from, time.reveal] + r.to_a
    end
  end

  # forward responses to the original requestor node
  bloom :responses do

    get_count <= (kvget_response_chan * waiting).lefts(:reqid => :reqid){|k,w| [k.to_a[2],k.to_a[1],k.to_a[3],k.to_a[4][0],k.to_a[4][1]]}
    put_count <= (kv_acks_chan * waiting).lefts(:reqid => :reqid) {|k, w| [k.reqid, k.t] }
    kv_acks <= put_count.argmax([], :time) do |c|
      [c.reqid] if put_count.to_a.length >= (member.to_a.length * r_w.to_a[0][1])
    end
    kvget_response <= get_count.argmax([], :oldtime) do |c|
      [c.reqid, c.key, c.value] if get_count.to_a.length >= (member.to_a.length * r_w.to_a[0][0])
    end
    #stdio <~ [[kv_acks.to_a.length]]
    waiting <- waiting do |w|
      w if kv_acks.to_a.length > 0 or kvget_response.to_a.length > 0
    end
    get_count <- get_count do |g|
      g if kv_acks.to_a.length > 0 or kvget_response.to_a.length > 0
    end
    put_count <- put_count do |p|
      p if kv_acks.to_a.length > 0 or kvget_response.to_a.length > 0
    end
  end

  bloom :timing do
    time <= [1] #always at least time 1 to counter it starting at -inf
    time <+ (time+1)
    time <= kvget_response_chan{|k| k.to_a[1]} #pushes the time of all of the gets to the clock
    time <= kvput_chan{|k| k.to_a[2]}
    time <= kvget_chan{|k| k.to_a[2]}
    time <= kv_acks_chan{|k| k.to_a[2]}
  end

  bloom :testing do
    #stdio <~ waiting.inspected
    #stdio <~ kv_acks_chan.inspected
    stdio <~ get_count.inspected
  end

end