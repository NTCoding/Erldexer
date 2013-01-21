-module(publisher).

-export([start/0]).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

start() ->
	spawn(fun() -> 
					subscribe_to_batcher(),
					publish() 
		  end).


subscribe_to_batcher() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
	{ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertbatches">>}),
	amqp_channel:subscribe(Channel, #'basic.consume'{queue = <<"upsertbatches">>, no_ack=true}, self()),
	receive
        #'basic.consume_ok'{} -> 
								io:format("Received ok signal~n"),
								ok
    end.


publish() ->
	Batch = next_batch(),
	publish_batch(Batch),
	timer:sleep(1000 * 30),
	publish().	


publish_batch(nobatch) ->
	io:format("No batches available to publish. Will look again in 30 seconds~n");

publish_batch(Batch) -> 
	io:format("Published batch to data store. Next batch in 30 seconds~n").
	
	
next_batch() -> 
	receive
		{#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
			Data = binary_to_term(Payload),			
			io:format("Received a batch: ~n~p~n", [Data]),
			Data
	after 1000 ->
		nobatch
	end.
	

