-module(job_producer).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

-export([produce/1, produce_priority_jobs/1]).

produce(Amount) -> 
	{Connection, Channel} = open_channel(),
	produce(Amount, Channel, Connection, normal_priority).


produce_priority_jobs(Amount) -> 
	{Connection, Channel} = open_channel(),
	produce(Amount, Channel, Connection, high_priority).
	

produce(Amount, Channel, Connection, Priority) when Amount > 0 ->
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertjobs">>},
	Binary = term_to_binary(track_details_for(Amount, Priority)),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:cast(Channel, Publish, Message),
	io:format("produced job. ~p left to go~n", [Amount - 1]),
	produce(Amount - 1, Channel, Connection, Priority);
	
produce(0, Channel, Connection, _) -> 
	io:format("Completed producing jobs. Closing connection~n"),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
	

open_channel() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertjobs">>}),
	{Connection, Channel}.


track_details_for(Number, Priority) ->
	{
		{priority, Priority},
		{details,
			{title, "Track " ++ Number},
			{release, "Release " ++ Number},
			{artist, "Artist " ++ Number},
			{price, "99"},
			{released, "01/01/2012"}
		}		
	}.
	



