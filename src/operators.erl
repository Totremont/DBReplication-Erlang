-module(operators).
-include("types.hrl").
-export([put/2,delete/2,get/2,size/1,inspect/1,getNewerDate/2,shuffle/1]).

% Returns {status,Database,#data}
put({Key,Value,Timestamp}, Database) ->
    case maps:find(Key, Database) of
        {ok, Data = #data{}} -> 
            case canOverride(Data,Timestamp) of
                true -> 
                    NewData = Data#data{value = Value, timestamp = Timestamp},
                    {ok,maps:update(Key, NewData, Database),NewData};
                false -> {ko,Database,Data}
            end;
        _ ->
            NewData = #data{value = Value, timestamp = Timestamp}, 
            {ok,maps:put(Key, NewData, Database),NewData}
    end.

delete({Key,Timestamp},Database) ->
    case maps:find(Key, Database) of
        {ok, Data = #data{status = removed}} -> {ko,Database,Data};
        {ok, Data = #data{}} ->
            case canOverride(Data,Timestamp) of
                true -> 
                    NewData = Data#data{status = removed, timestamp = Timestamp},
                    {ok,maps:update(Key, NewData, Database),NewData};
                false -> {ko,Database, Data}
            end;
        _ -> {notfound,Database,nil}
    end.
    
get(Key,Database) -> 
    case maps:find(Key, Database) of
        {ok, Data = #data{status = removed}} -> {ko,Database,Data};
        {ok, Data = #data{}} -> {ok,Database,Data};
        _ -> {notfound,Database,nil}
    end.

size(Database) ->
    PresentEntries = maps:filter(fun(_,#data{status = Status}) -> Status =:= present end,Database),
    {ok,Database,maps:size(PresentEntries)}.

%% Get database info
inspect(Database) -> {ok,Database,Database}.


% Utils
canOverride(#data{timestamp = Stored},TimeStamp) ->
    Diff = calendar:datetime_to_gregorian_seconds(TimeStamp) - calendar:datetime_to_gregorian_seconds(Stored),
    Diff > 0.

% If equal, Date1 takes precedence
getNewerDate(Date1,Date2) ->
Diff = calendar:datetime_to_gregorian_seconds(Date1) - calendar:datetime_to_gregorian_seconds(Date2),
if Diff >= 0 ->
    Date1;
true -> Date2 end.

% Randomize List
shuffle(List) ->
%% Determine the log n portion then randomize the list.
randomize(round(math:log(length(List)) + 0.5), List).

randomize(1, List) ->
randomize(List);
randomize(T, List) ->
lists:foldl(fun(_E, Acc) ->
    randomize(Acc)
end, randomize(List), lists:seq(1, (T - 1))).

randomize(List) ->
D = lists:map(fun(A) -> 
    {rand:uniform(), A}
    end, List),

{_, D1} = lists:unzip(lists:keysort(1, D)),
D1.



