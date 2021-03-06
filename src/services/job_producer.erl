-module(job_producer).

-include_lib("lib/amqp_client/include/amqp_client.hrl").

-export([produce/2, produce_priority_jobs/2]).

produce(Amount, Shop) -> 
	{Connection, Channel} = open_channel(),
	produce(Amount, Channel, Connection, normal_priority, Shop).


produce_priority_jobs(Amount, Shop) -> 
	{Connection, Channel} = open_channel(),
	produce(Amount, Channel, Connection, high_priority, Shop).
	

produce(Amount, Channel, Connection, Priority, Shop) when Amount > 0 ->
	Publish = #'basic.publish'{ exchange = <<"">>, routing_key = <<"upsertjobs">>},
	Binary = term_to_binary(track_details_for(Amount, Priority, Shop)),
	Message = #amqp_msg{payload = Binary },
	amqp_channel:cast(Channel, Publish, Message),
	io:format("produced job. ~p left to go~n", [Amount - 1]),
	produce(Amount - 1, Channel, Connection, Priority, Shop);
	
produce(0, Channel, Connection, _, _) -> 
	io:format("Completed producing jobs. Closing connection~n"),
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
	

open_channel() ->
	{ok, Connection} = amqp_connection:start(#amqp_params_network{host = "localhost"}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
	amqp_channel:call(Channel, #'queue.declare'{queue = <<"upsertjobs">>}),
	{Connection, Channel}.


track_details_for(Number, Priority, Shop) ->
	{
		{priority, Priority},
		{details,
			{title, "Track " ++ Number},
			{release, "Release " ++ Number},
			{artist, "Artist " ++ Number},
			{price, "99"},
			{released, "01/01/2012"},
			{shop, Shop}
		}		
	}.
	



