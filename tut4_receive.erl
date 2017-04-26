-module(tut4_receive).
-export([receive_msg/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

receive_msg() ->
    {_Connection, Channel, Queue} = create_queue("localhost", [<<"info">>, <<"warning">>, <<"error">>]),
    io:format(" [*] Waiting for logs. You have 30 seconds. To exit press CTRL+C~n"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue,
                                                     no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    loop(Channel).

loop(Channel) ->
    receive
        {#'basic.deliver'{routing_key = <<"info">>}, #amqp_msg{payload = Body}} ->
            io:format(" [x] Received ~s:'~s'.~n", [<<"info">>,Body]),
            io:format("     Console output only.~n"),
            loop(Channel);
        {#'basic.deliver'{routing_key = RoutingKey}, #amqp_msg{payload = Body}} ->
            io:format(" [x] Received ~s:'~s'.~n", [RoutingKey,Body]),
            io:format("     Console output and Log file.~n"),
            loop(Channel)
    after 30000 -> ok
    end.

create_queue(Host, Severities) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"direct_logs">>,
                                                   type = <<"direct">>}),
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),

    lists:map(
      fun(Severity) ->
              amqp_channel:call(Channel, #'queue.bind'{exchange = <<"direct_logs">>,
                                             routing_key = Severity,
                                             queue = Queue})
      end, Severities),
    {Connection, Channel, Queue}.
