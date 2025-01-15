-module(operators).
-include("../types.hrl").
-export([put/2,delete/2,get/2,size/1]).


put({Key,Value,Timestamp}, Database) ->
    case maps:find(Key, Database) of
        {ok, Data = #data{}} -> 
            case canOverride(Data,Timestamp) of
                true -> 
                    {ok,maps:update(Key, Data#data{value = Value, timestamp = Timestamp}, Database)};
                false -> {ko,Database}
            end;
        _ -> {ok,maps:put(Key, #data{value = Value,timestamp = Timestamp}, Database)}
    end.

delete({Key,Timestamp},Database) ->
    case maps:find(Key, Database) of
        {ok, #data{status = removed}} -> ko;
        {ok, Data = #data{}} ->
            case canOverride(Data,Timestamp) of
                true -> 
                    {ok,maps:update(Key, Data#data{status = removed, timestamp = Timestamp}, Database)};
                false -> {ko,Database}
            end;
        _ -> notfound
    end.
    
get(Key,Database) -> 
    case maps:find(Key, Database) of
        {ok, #data{status = removed, timestamp = Timestamp}} -> {ko,Timestamp};
        {ok, Data = #data{}} -> {ok,Data#data.value, Data#data.timestamp};
        _ -> notfound
    end.

size(Database) -> maps:size(Database).


% Utils
canOverride(#data{timestamp = Stored},TimeStamp) ->
    Diff = calendar:datetime_to_gregorian_seconds(TimeStamp) - calendar:datetime_to_gregorian_seconds(Stored),
    Diff > 0.



