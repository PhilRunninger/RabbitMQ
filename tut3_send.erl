-module(tut3_send).
-export([send_msg/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

send_msg(Msg) ->
    {Connection, Channel} = create_queue("localhost"),
    amqp_channel:cast(Channel,
                      #'basic.publish'{exchange = <<"logs">>},
                      #amqp_msg{payload = Msg}),
    io:format(" [x] Sent '~s'.~n", [Msg]),
    close_connection(Connection, Channel),
    ok.

create_queue(Host) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"logs">>,
                                                   type = <<"fanout">>}),
    {Connection, Channel}.

close_connection(Connection, Channel) ->
    ok = amqp_channel:close(Channel),
    ok = amqp_connection:close(Connection).
