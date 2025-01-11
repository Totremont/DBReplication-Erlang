-module(main).
-export([start/0,loop/1]).

%% Database stores values as {Key,{Value,Timestamp}}
%% Calendar : {{Year, Month, Day}, {Hour, Minute, Second}}
start() -> spawn(?MODULE,loop,[orddict:new()]). 


loop(Database) -> 
    receive
        {Pid, put, Arg = {Key,Value,NewDate}} -> 
            case put0(Arg,Database) of
                {ok,NewData} -> Pid ! {ok,NewDate}, loop(NewData);
                {ko,_} -> Pid ! {ko,"Stored date is newer"}, loop(Database) 
            end;
        shutdown -> exit(normal)
    end.

put0({Key,Value,NewDate}, Database) -> 
    case orddict:find(Key,Database) of 
        {ok, {_,SavedTime}} -> % Key is present
            case getNewerDate(NewDate,SavedTime) of % Comparing dates
                NewDate -> {ok,orddict:update(Key,fun(_) -> {Value,NewDate} end, Database)};
                SavedTime -> {ko,Database} 
            end; 
        error ->                % Key is not present
            {ok,orddict:store(Key, {Value,NewDate}, Database)} 
    end.


% Utilities
getNewerDate(Date1, Date2) -> 
    Diff = calendar:datetime_to_gregorian_seconds(Date1) - calendar:datetime_to_gregorian_seconds(Date2),
    if Diff >= 0 -> Date1;
    true -> Date2 end.