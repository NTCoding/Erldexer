-module(batcher).

-export([batch/0]).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

batch() -> 
	spawn(fun() -> begin_batching() end).


begin_batching() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
	Channel = open_channel(Connection),
	loop(Channel, [], []).


open_channel(Connection) ->
	{ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:subscribe(Channel, #'basic.consume'{queue = <<"upsertjobs">>, no_ack=true}, self()),
	receive
        #'basic.consume_ok'{} -> 
								io:format("Received ok signal~n"),
								ok
    end,
	Channel.


loop(Channel, Batch, PriorityBatch) when erlang:length(Batch) >= 50 -> 
	publish(Batch, Channel),
	loop(Channel, [], PriorityBatch);

loop(Channel, Batch, PriorityBatch) when erlang:length(PriorityBatch) >= 5 -> %% assumes priority batches are frequent - talk to the business
	publish_priority_batch(PriorityBatch, Channel),
	loop(Channel, Batch, []);

loop(Channel, Batch, PriorityBatch) ->
	receive
		{#'basic.deliver'{}, #amqp_msg{payload = Payload}} ->
			Track = [binary_to_term(Payload)],
			case is_priority(Track) of
			
				true -> 
					NewPriorityBatch = lists:append(PriorityBatch, Track),
					loop(Channel, Batch, NewPriorityBatch);

				false ->
					NewBatch = lists:append(Batch, Track),
					loop(Channel, NewBatch, PriorityBatch)	

			end
					
	after 30 ->
		loop(Channel, Batch, PriorityBatch)
	end.


is_priority([{{priority, high_priority}, _}|T]) -> true;

is_priority([{{priority, normal_priority}, _}|T]) -> false.


publish(Batch, Channel) -> 
	Binary = term_to_binary(Batch),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertbatches">>}),
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertbatches">>},
	amqp_channel:cast(Channel, Publish, Message),
	io:format("Batcher created a batch ~n").


publish_priority_batch(Batch, Channel) -> 
	Binary = term_to_binary(Batch),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertprioritybatches">>}),
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertprioritybatches">>},
	amqp_channel:cast(Channel, Publish, Message),
	io:format("Batcher created a priority batch ~n").
	
	


	

	
