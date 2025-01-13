-module(main).
-export([start/0, start_monitor/0, loop/1]).
-record(data,{status = present, value, timestamp}).  %%Status = present | removed

%% Database stores values as {Key,{present | removed, {Value,Timestamp}}}
%% Calendar : {{Year, Month, Day}, {Hour, Minute, Second}}
start() -> spawn(?MODULE,loop,[orddict:new()]). 
start_monitor() -> spawn_monitor(?MODULE,loop,[orddict:new()]).


loop(Database) -> 
    receive

        {Pid, put, Arg = {_,_,Timestamp}} -> 
            case put0(Arg,Database) of
                {ok,Udatabase} -> Pid ! {ok,Timestamp}, loop(Udatabase);
                {ko,SavedDate} -> Pid ! {ko,SavedDate}, loop(Database) 
            end;
        
        {Pid,get,Key} -> 
            Pid ! get0(Key,Database), loop(Database);

        {Pid,del,{Key,Timestamp}} -> 
            case rem0(Key,Timestamp,Database) of
                {ok,Udatabase} -> Pid ! {ok,Timestamp}, loop(Udatabase);
                {ko,SavedDate} -> Pid ! {ko,SavedDate}, loop(Database);
                notfound -> Pid ! notfound, loop(Database)
            end;

        shutdown -> exit(normal)
    end.

%% {ok,Udatabase} | {ko,Timestamp}
put0({Key,Value,NewDate}, Database) -> 
    case orddict:find(Key,Database) of 
        {ok, Data = #data{timestamp = SavedDate}} ->    % Key is present
            case getNewerDate(NewDate,SavedDate) of     % Comparing dates
                NewDate -> 
                    {
                        ok,
                        orddict:update(   
                        Key,
                        fun(_) -> Data#data{status = present, value = Value,timestamp = NewDate} end, 
                        Database
                        )
                    };
                SavedDate -> {ko,SavedDate}
            end; 
        error ->                % Key is not present
            {
                ok,orddict:store(
                Key, 
                #data{status = present, value = Value, timestamp = NewDate}, 
                Database)
            } 
    end.

%% {ok,Timestamp} | {ko, Timestamp} | notfound
get0(Key,Database) -> 
    case orddict:find(Key,Database) of 
        {ok, #data{status = removed, timestamp = Timestamp}} -> % Logically removed
            {ko,Timestamp};
        {ok, #data{value = Val, timestamp = Timestamp}} ->
            {ok,{Val,Timestamp}};    
        error -> notfound
    end.

% {ok,Udatabase} | {ko,Timestamp} | notfound
rem0(Key,NewDate,Database) ->
    case orddict:find(Key,Database) of 
        {ok, #data{status = removed}} -> % Already removed
            {ok,NewDate};
        {ok, Data = #data{timestamp = SavedDate}} ->
            case getNewerDate(NewDate, SavedDate) of
                NewDate -> 
                    {
                        ok,
                        orddict:update(   
                        Key,
                        fun(_) -> Data#data{status = removed, timestamp = NewDate} end, 
                        Database
                        )
                    };
                _ -> {ko,SavedDate} 
            end;              
        error -> notfound
    end.

size0(Database) -> orddict:size(Database).    
% Utilities
getNewerDate(Date1, Date2) -> 
    Diff = calendar:datetime_to_gregorian_seconds(Date1) - calendar:datetime_to_gregorian_seconds(Date2),
    if Diff >= 0 -> Date1;
    true -> Date2 end.
