-module(tut5_receive).
-export([receive_msg/1]).

% Example Usage:
%   This module receives status messages from your car's computer. To indicate
%   you care only about critical messages from any system or any message from
%   your engine, do the following.
%
% 1. erl -pa */ebin
% 2. tut5_receive:receive_msg([<<"*.critical">>, <<"engine.*">>]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

receive_msg(BindingKeys) ->
    {_Connection, Channel, Queue} = create_queue("localhost", BindingKeys),
    io:format(" [*] Waiting 30 seconds for each message. To exit, press CTRL+C~n"),
    amqp_channel:subscribe(Channel, #'basic.consume'{queue = Queue, no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,
    loop(Channel).

loop(Channel) ->
    receive
        {#'basic.deliver'{routing_key = RoutingKey}, #amqp_msg{payload = Body}} ->
            io:format(" [x] Received ~s:'~s'.~n", [RoutingKey,Body]),
            loop(Channel)
    after 30000 -> ok
    end.

create_queue(Host, BindingKeys) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_network{host = Host}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = <<"topic_logs">>, type = <<"topic">>}),
    #'queue.declare_ok'{queue=Queue} = amqp_channel:call(Channel, #'queue.declare'{exclusive = true}),
    [amqp_channel:call(Channel, #'queue.bind'{exchange = <<"topic_logs">>,
                                              routing_key = BindingKey,
                                              queue = Queue})
     || BindingKey <- BindingKeys],
    {Connection, Channel, Queue}.
