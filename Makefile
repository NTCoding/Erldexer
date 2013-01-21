all: compile start

compile:
	@./rebar compile

start:
	erl -pa ebin ./lib/amqp_client-2.7.0/ebin ./rabbit_common/ebin src
