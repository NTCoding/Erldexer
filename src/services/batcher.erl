-module(batcher).

-export([batch/0]).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

-define(BATCHSIZE, 50).

batch() -> 
	spawn(fun() -> begin_batching() end).


begin_batching() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
	{ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertjobs">>}),
	amqp_channel:subscribe(Channel, #'basic.consume'{queue = <<"upsertjobs">>, no_ack=true}, self()),
	
	receive
        #'basic.consume_ok'{} -> 
								io:format("Received ok signal~n"),
								ok
    end,
	
	loop(Channel, []).


loop(Channel, Batch) when erlang:length(Batch) >= 50 -> 
	publish(Batch, Channel),
	loop(Channel, []);

loop(Channel, Batch) ->
	receive
		{#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
			io:format(" Received a message. About to batch it ~n"),
			Track = [binary_to_term(Payload)],
			NewBatch = lists:append(Batch, Track),
			loop(Channel, NewBatch)			
	after 30 ->
		loop(Channel, Batch)
	end.


publish(Batch, Channel) -> 
	Binary = term_to_binary(Batch),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertbatches">>}),
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertbatches">>},
	amqp_channel:cast(Channel, Publish, Message),
	io:format("Batcher published a batch ~n").
	
	


	

	
