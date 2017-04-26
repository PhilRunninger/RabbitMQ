-module(tut1).
-export([send_msg/1, receive_msg/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

send_msg(Msg) ->
    Queue = <<"hello">>,
    {Connection, Channel} = create_queue("localhost", Queue),
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = Queue},
                      #amqp_msg{payload = Msg}),
    io:format(" [x] Sent '~s'.~n", [Msg]),
    close_connection(Connection, Channel),
    ok.

receive_msg() ->
    Queue = <<"hello">>,
    {_Connection, Channel} = create_queue("localhost", Queue),
    io:format(" [*] Waiting for messages. To exit press CTRL+C~n"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    loop(Channel).

loop(Channel) ->
    receive
        {#'basic.deliver'{}, #amqp_msg{payload=Body}} ->
            io:format(" [x] Received '~s'.~n", [Body]),
            loop(Channel)
    end.

create_queue(Host, Queue) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'queue.declare'{queue = Queue}),
    {Connection, Channel}.

close_connection(Connection, Channel) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
