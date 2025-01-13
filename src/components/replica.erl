-module(replica).

-export([start/2]).

start(_, Name) ->
    register(Name, self()),
    io:format("[start/2] Database [~p] created with PID: ~p.~n", [Name, self()]),
    loop(Name, maps:new()).

loop(SelfName, Map) -> % SelfName es el atom con el que se registró
    receive
        {put, Src, Key, Value, Timestamp} -> 
            case maps:find(Key, Map) of
                error -> 
                    NewMap = maps:put(Key, {Value, Timestamp, false}, Map),
                    Src ! {ok, NewMap},
                    loop(SelfName, NewMap);
                {ok, {_, TimestampStored, Deleted}} -> 
                    case (calendar:datetime_to_gregorian_seconds(TimestampStored) < calendar:datetime_to_gregorian_seconds(Timestamp) orelse Deleted) of
                        true -> 
                            NewMap = maps:update(Key, {Value, Timestamp, false}, Map),
                            Src ! {ok, NewMap},
                            loop(SelfName, Map);
                        false -> 
                            Src ! {error, Map},
                            loop(SelfName, Map)
                    end
            end;
        {put, Src, Key, Value, Timestamp, one, Coord, Replics} ->
            case Coord == SelfName of
                true ->
                    io:format("[put/5] Soy ~p y soy el coordinador de la operación put.~n", [SelfName]),
                    sendPutTo(Replics, Src, Key, Value, Timestamp)
            end,
            Src ! {ok, ""},
            loop(SelfName, Map);
        % {put, Src, Key, Value, Timestamp, quorum, Coord, Replics} ->
        %     case Coord == SelfName of
        %         true ->
        %             io:format("[put/5] Soy ~p y soy el coordinador de la operación put.~n", [SelfName])
        %     end,
        %     Src ! {ok, ""},
        %     loop(SelfName, Map);
        % {put, Src, Key, Value, Timestamp, all, Coord, Replics} ->
        %     case Coord == SelfName of
        %         true ->
        %             io:format("[put/5] Soy ~p y soy el coordinador de la operación put.~n", [SelfName])
        %     end,
        %     Src ! {ok, ""},
        %     loop(SelfName, Map);
        {ok, _} ->
            ok;
        {error, _} ->
            ok;
        {_} ->
            io:format("Soy el PID ~p y recibí un mensaje.", [self()]),
            loop(SelfName, Map)
    end.
sendPutTo([Last], Src, Key, Value, Timestamp) ->
    Last ! {put, Src, Key, Value, Timestamp};
sendPutTo([Head | Tail], Src, Key, Value, Timestamp) ->
    Head ! {put, Src, Key, Value, Timestamp},
    sendPutTo(Tail, Src, Key, Value, Timestamp).
