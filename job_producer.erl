-module(job_producer).

-export([produce/1]).

produce(Amount) when Amount > 0 ->
	io:format("produced job. ~p left to go~n", [Amount - 1]),
	produce(Amount - 1);
	
produce(0) -> io:format("Completed producing jobs~n").

