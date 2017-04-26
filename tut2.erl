-module(tut2).
-export([send_msg/1, receive_msg/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

queue() -> <<"task_queue">>.

send_msg(Msg) ->
    {Connection, Channel} = create_queue("localhost", queue()),
    amqp_channel:cast(Channel,
                      #'basic.publish'{
                         exchange = <<"">>,
                         routing_key = queue()},
                      #amqp_msg{props = #'P_basic'{delivery_mode = 2},
                                payload = Msg}),
    io:format(" [x] Sent '~s'.~n", [Msg]),
    close_connection(Connection, Channel),
    ok.

receive_msg() ->
    {_Connection, Channel} = create_queue("localhost", queue()),
    io:format(" [*] Waiting for messages. To exit press CTRL+C~n"),
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = queue()}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    loop(Channel).

loop(Channel) ->
    receive
        {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{payload=Body}} ->
            Dots = length([C || C <- binary_to_list(Body), C == $.]),
            io:format(" [x] Received '~s'.~n", [Body]),
            receive
            after
                Dots * 1000 -> ok
            end,
            io:format(" [x] ~p is done.~n", [Body]),
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag=Tag}),
            loop(Channel)
    end.

create_queue(Host, Queue) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'queue.declare'{queue = Queue, durable=true}),
    {Connection, Channel}.

close_connection(Connection, Channel) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
