-module(graphite).
-behaviour(gen_server).

-include("egc_internal.hrl").

-export([start_link/4, get/2, get/4, send/2, info/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {host, port, sock, prefix, last_reconnect = 0, reconnect_after, last_reconnect_warn = 0, last_missing_socket_warn = 0}).

get(Name, Key) ->
    get(Name, Key, undefined, undefined).
get(Name, Key, Start, End) ->
    gen_server:call(Name, {get, Key, Start, End}).

send(Name, Stats) when is_list(Stats) ->
    gen_server:cast(Name, {send, Stats}).

info(Name) ->
    gen_server:call(Name, info).

start_link(Name, Host, Port, Prefix) when is_atom(Name), is_list(Host), is_integer(Port), is_list(Prefix) ->
    gen_server:start_link({local, Name}, ?MODULE, [Host, Port, Prefix], []).

init([Host, Port, Prefix]) ->
    {ok, p_reconnect(#state{
       host = Host,
       port = Port,
       prefix = Prefix,
       reconnect_after = app_env(reconnect_after, 300)
      })}.

p_reconnect(#state{host = H, port = P, sock = undefined, reconnect_after = RA, last_reconnect = LR, last_reconnect_warn = LRW} = State) ->
    Now = p_now(),
    case ( Now - LR ) > RA of
        true ->
            case gen_tcp:connect(H, P, [list, {packet, 0}]) of
                {ok, S} ->
                    State#state{sock = S};
                {error, E} ->
		    State#state{
		      last_reconnect_warn = warn_once("error ~p connecting ~p:~p", [E, H, P], LRW),
		      last_reconnect = Now
		     }
            end;
        false ->
            State
    end;
p_reconnect(#state{sock = S} = State) ->
    ok = gen_tcp:close(S),
    p_reconnect(State#state{sock = undefined}).

handle_call({get, Key, Start, End}, _From, State) ->
    {reply, p_get(Key, Start, End, State), State};
handle_call(info, _From, State) -> {reply, p_info(State), State}.

handle_cast({send, Stats}, State) -> {noreply, p_send(Stats, State)}.

handle_info({tcp_closed, Sock}, #state{sock = Sock} = State) ->
    {noreply, p_reconnect(State#state{sock = undefined})};
handle_info({tcp_error, Sock, _E}, #state{sock = Sock} = State) ->
    {noreply, p_reconnect(State#state{sock = undefined})}.

terminate(_, _State) ->
    ok.

code_change(_, _, State) ->
    {ok, State}.



p_send(_Stats, #state{sock = undefined, last_missing_socket_warn = LMSW} = State) ->
    p_reconnect(State#state{last_missing_socket_warn = warn_once("unable to send on missing socket", [], LMSW)});
p_send(Stats, #state{sock = Sock} = State) ->
    Ts = now_unix(),
    M = p_graphite_message(Stats, Ts, State),
    case gen_tcp:send(Sock, M) of
        ok ->
	    State;
        {error, closed} ->
            p_reconnect(State)
    end.

p_info(#state{sock = S, host = H, port = Po, prefix = Pr}) ->
    [
     {connected, if S == undefined -> false ; true -> true end},
     {host, H},
     {port, Po},
     {prefix, Pr}
    ].

p_graphite_message(Stats, Ts, #state{} = S) -> p_graphite_message(Stats, Ts, "",  S).
p_graphite_message([], _, S, _) -> S;
p_graphite_message([{K, V} | Stats], Ts, S, #state{prefix = P} = State) ->
    p_graphite_message(Stats, Ts, S ++ lists:flatten(io_lib:format("~s.~s ~p ~p~n", [P, K, V, Ts])), State).

p_graphite_date(I) when is_integer(I) ->
    p_graphite_date(calendar:gregorian_seconds_to_datetime(I));
p_graphite_date({{Y, Mo, D}, {H, Mi, _S}}) when is_integer(Y), is_integer(Mo), is_integer(D), is_integer(H), is_integer(Mi) ->
    integer_to_list(H) ++ ":" ++ integer_to_list(Mi) ++ "_" ++ integer_to_list(Y) ++ integer_to_list(Mo) ++ integer_to_list(D).

p_get(Key, Start, End, #state{host = Host, prefix = Pr}) ->
    URL = "http://" ++ Host ++ "/?target=" ++ Pr ++ "." ++ Key ++
	if
	    Start == undefined ->
		"";
	    true ->
		"&from=" ++ p_graphite_date(Start)
	end ++ 
	if
	    End == undefined ->
		"";
	    true ->
		"&until=" ++ p_graphite_date(End)
	end,
    case httpc:request(get, {URL, []}, [], []) of
	{ok, {{_, 200, _}, _Headers, Body}} ->
	    jiffy:decode(Body)
    end.

app_env(Key, Default) ->
    case application:get_env(egc, Key) of
	undefined ->
	    Default;
	{ok, Value} ->
	    Value
    end.

warn_once(F, A, L) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    case Now - L > ?WARN_EVERY of
	false ->
	    ok;
	true ->
	    ?warn(F, A)
    end,
    Now.

p_now() ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time()).
now_unix() ->
    p_now() - (719528*24*3600).
