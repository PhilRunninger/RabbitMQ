-module(tut3_receive).
-export([receive_msg/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

receive_msg() ->
    {_Connection, Channel, Queue} = create_queue("localhost"),
    io:format(" [*] Waiting for logs. To exit press CTRL+C~n"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue,
                                                     no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    loop(Channel).

loop(Channel) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload=Body}} ->
            Dots = length([C || C <- binary_to_list(Body), C == $.]),
            io:format(" [x] Received '~s'.~n", [Body]),
            receive
            after
                Dots * 1000 -> ok
            end,
            io:format(" [x] Done~n"),
            loop(Channel)
    end.

create_queue(Host) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"logs">>,
                                                   type = <<"fanout">>}),
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    amqp_channel:call(Channel, #'queue.bind'{exchange = <<"logs">>,
                                             queue = Queue}),
    {Connection, Channel, Queue}.
