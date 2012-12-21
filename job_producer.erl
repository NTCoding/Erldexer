-module(job_producer).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

-export([produce/1]).

%% erl -pa ./lib/amqp_client-2.7.0/ebin ./rabbit_common/ebin %%
produce(Amount) -> 
	Channel = open_channel(),
	produce(Amount, Channel).
	

produce(Amount, Channel) when Amount > 0 ->
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertjobs">>},
	Binary = term_to_binary(track_details_for(Amount)),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:cast(Channel, Publish, Message),
								
	io:format("produced job. ~p left to go~n", [Amount - 1]),
	produce(Amount - 1); %% might need to close connection if getting any errors &&
	
produce(0, Channel) -> io:format("Completed producing jobs~n").


open_channel() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertjobs">>}),
	Channel.


track_details_for(Number) ->
	{
		{title, "Track " ++ Number},
		{release, "Release " ++ Number},
		{artist, "Artist " ++ Number},
		{price, "99"},
		{released, "01/01/2012"}
	}.
	



