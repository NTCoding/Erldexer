-module(publisher).

-export([start/0]).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

start() -> spawn(fun() -> publish(channel()) end).


channel() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
	{ok, Channel} = amqp_connection:open_channel(Connection),
	Channel.


%% publishing
publish(Channel) ->
	Batch = next_batch(Channel),
	publish_batch(Batch, Channel),
	timer:sleep(1000 * 30),
	publish(Channel).	

publish_batch(no_batch, Channel) ->
	io:format("No batches available to publish. Will look again in 30 seconds~n");

publish_batch({Content, Tag}, Channel) -> 
	amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
	io:format("Published batch to data store & sent ack to queue. Next batch in 30 seconds~n").


%% getting batch
next_batch(Channel) ->
	PriorityBatch = read_priority_batch(Channel),
	next_batch(PriorityBatch, Channel).

next_batch(no_priority_batch_available, Channel) -> read_normal_batch(Channel);

next_batch(PriorityBatch, Channel) -> PriorityBatch.


%% reading from queue
read_priority_batch(Channel) ->
	read_batch_or_default(Channel, <<"upsertprioritybatches">>, no_priority_batch_available).

read_normal_batch(Channel) ->
	read_batch_or_default(Channel, <<"upsertbatches">>, no_batch).	

read_batch_or_default(Channel, Queue, Default) ->
	amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
	case amqp_channel:call(Channel, #'basic.get'{queue = Queue}) of

			{#'basic.get_ok'{delivery_tag = Tag}, Content} ->	{Content, Tag};

			#'basic.get_empty'{} -> default

	end.
	

	

	

