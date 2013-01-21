-module(batcher).

-behaviour(gen_server).

-export([start_link/0, init/1, handle_cast/2, handle_info/2, code_change/3, handle_call/3, terminate/2]).

-include_lib("lib/amqp_client/include/amqp_client.hrl").


start_link() -> 
	gen_server:start_link(?MODULE, [], []).


init([]) ->
	Channel = create_channel(),
	{ok, {Channel, [], []}}.


create_channel() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
	Channel = open_channel(Connection),
	Channel.


open_channel(Connection) ->
	{ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:subscribe(Channel, #'basic.consume'{queue = <<"upsertjobs">>, no_ack=true}, self()),
	receive
        #'basic.consume_ok'{} -> 
								io:format("Received ok signal~n"),
								ok
    end,
	Channel.


%% receive batch job
handle_info({#'basic.deliver'{}, #amqp_msg{payload = Payload}}, State) ->
	Track = [binary_to_term(Payload)],
	NewState = append_to_batch(Track, State),
	FinalState = publish_if_threshold_met(NewState),
	{noreply, FinalState}.
	

append_to_batch(Track, {Channel, Batch, PriorityBatch}) ->
	case is_priority(Track) of
		true -> 
			NewPriorityBatch = lists:append(PriorityBatch, Track),
			{Channel, Batch, NewPriorityBatch};

		false ->
			NewBatch = lists:append(Batch, Track),
			{Channel, NewBatch, PriorityBatch}
	end.


is_priority([{{priority, high_priority}, _}|_]) -> true;

is_priority([{{priority, normal_priority}, _}|_]) -> false.


publish_if_threshold_met({Channel, Batch, PriorityBatch}) when erlang:length(Batch) >= 50 ->
	publish(Batch, Channel), 
	{Channel, [], PriorityBatch};

publish_if_threshold_met({Channel, Batch, PriorityBatch}) when erlang:length(PriorityBatch) >= 5 ->
	publish_priority_batch(PriorityBatch, Channel),
	{Channel, Batch, []};

publish_if_threshold_met(State) ->
	State.


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


%% Un-needed OTP functions and callbacks
handle_cast(_, _) -> ok.

handle_call(_, _, _) -> ok.

code_change(_, _, _) -> ok.

terminate(_, _) -> ok.

