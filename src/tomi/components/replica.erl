-module(replica).

-export([start/2]).

start(_, Name) -> % El primer parametro es el pid del handler
    register(Name, self()),
    io:format("[start/2] Database [~p] created with PID: ~p.~n", [Name, self()]),
    listen(maps:new(), Name).

listen(Map, SelfName) ->
    receive
        {put, Src, Key, Value, Timestamp} -> 
            case maps:find(Key, Map) of
                error -> 
                    NewMap = maps:put(Key, {Value, Timestamp, false}, Map),
                    Src ! {ok, SelfName},
                    listen(NewMap, SelfName);
                {ok, {_, TimestampStored, Deleted}} -> 
                    case (calendar:datetime_to_gregorian_seconds(TimestampStored) < calendar:datetime_to_gregorian_seconds(Timestamp) orelse Deleted) of
                        true -> 
                            NewMap = maps:update(Key, {Value, Timestamp, false}, Map),
                            Src ! {ok, SelfName},
                            listen(NewMap, SelfName);
                        false -> 
                            Src ! {error, SelfName},
                            listen(Map, SelfName)
                    end
            end;

        {remove, Src, Key, Timestamp} -> 
            case maps:find(Key, Map) of
                error -> 
                    Src ! {error, "No key found."};
                {ok, {Value, TimestampStored, Deleted}} -> 
                    case {calendar:datetime_to_gregorian_seconds(TimestampStored) < calendar:datetime_to_gregorian_seconds(Timestamp), Deleted} of
                        {true, false} -> 
                            NewMap = maps:update(Key, {Value, Timestamp, true}, Map),
                            Src ! {ok, NewMap},
                            listen(NewMap, SelfName);
                        _ -> 
                            Src ! {error, Map},
                            listen(Map, SelfName)
                    end
            end;

        {get, Src, Key} ->
            case maps:find(Key, Map) of
                error ->
                    Src ! {notfound};
                {ok, {Value, Timestamp, Deleted}} ->
                    case Deleted of
                        true ->
                            Src ! {ko, Timestamp};
                        false ->
                            Src ! {ok, Value, Timestamp}
                    end
            end,
            listen(Map, SelfName);

        {size, Src} ->
            Src ! {ok, maps:size(Map)},
            listen(Map, SelfName);
        {say, Src} ->
            Src ! {ok, Map},
            listen(Map, SelfName)
    end.


% loop(SelfName, Map) -> % SelfName es el atom con el que se registró
%     receive
%         {put, Src, Key, Value, Timestamp} -> 
%             case maps:find(Key, Map) of
%                 error -> 
%                     NewMap = maps:put(Key, {Value, Timestamp, false}, Map),
%                     Src ! {ok, NewMap},
%                     loop(SelfName, NewMap);
%                 {ok, {_, TimestampStored, Deleted}} -> 
%                     case (calendar:datetime_to_gregorian_seconds(TimestampStored) < calendar:datetime_to_gregorian_seconds(Timestamp) orelse Deleted) of
%                         true -> 
%                             NewMap = maps:update(Key, {Value, Timestamp, false}, Map),
%                             Src ! {ok, NewMap},
%                             loop(SelfName, Map);
%                         false -> 
%                             Src ! {error, Map},
%                             loop(SelfName, Map)
%                     end
%             end;
%         {_} ->
%             loop(SelfName, Map)
%     end.
% sendPutTo([Last], Src, Key, Value, Timestamp) ->
%     Last ! {put, Src, Key, Value, Timestamp};
% sendPutTo([Head | Tail], Src, Key, Value, Timestamp) ->
%     Head ! {put, Src, Key, Value, Timestamp},
%     sendPutTo(Tail, Src, Key, Value, Timestamp).

% wait([]) ->
%     ok;
% wait(List) ->
%     receive
%         {ok, Src} ->
%             NewList = lists:delete(Src, List),
%             wait(NewList)
%     end.