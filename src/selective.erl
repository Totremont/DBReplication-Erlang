-module(selective).
-include("types.hrl").
-define(TIMEOUT, 5).
-export([onConfirm/3,savePending/6,purge/2,nextTimeout/0]).


% Returns new {Table,Queue,Timeout}
savePending(Pid,MyResponse,Ref,Group,Queue,Table) ->
    if Group =:= [] ->
        Pid ! MyResponse,
        {Table,Queue};
    true -> 
        % 5 seconds after
        Timeout = nextTimeout(),
        {
            gb_trees:enter(Timeout, {Ref,Pid}, Table),
            maps:put(Ref, {Timeout,Group,MyResponse}, Queue),
            Timeout
        }
    end.

nextTimeout() -> calendar:datetime_to_gregorian_seconds(calendar:local_time()) + ?TIMEOUT.


% Queue is expected to by a map 
% where Key = Ref and Value = {Timeout,List,Response#repl}. List being the names of pending confirms.
% Returns {NewTable,NewQueue}
onConfirm(NewData = #repl{ref = Ref, name = Identity, timestamp = CurrTime}, Table, Queue) ->

    case maps:find(Ref, Queue) of
        {ok,{Timeout,Pending,BestResponse = #repl{timestamp = SavedTime}}} ->
            NewPending = lists:delete(Identity, Pending),
            NewResponse = 
            case operators:getNewerDate(SavedTime,CurrTime) of
                SavedTime -> BestResponse;
                CurrTime -> NewData
            end,
            if NewPending =:= [] ->
                verify({Timeout,Ref,NewResponse},Table,Queue);
            true ->
                {Table,maps:put(Ref, {Timeout,NewPending,NewResponse}, Queue)}
            end;
        % Deleted by a purge
        error -> {Table,Queue}
    end.
        

% Table is a balanced tree where keys are ordered by pending timeouts.
% Returns {NewTable,NewQueue}
verify({Timeout,Ref,BestResponse},Table,Queue) ->
    try gb_trees:get(Timeout, Table) of
        {Ref,Pid} -> 
            Pid ! BestResponse,
            {gb_trees:delete(Timeout, Table), maps:remove(Ref, Queue)}
    catch
        % Didnt exist or was already purged
        _:_ -> {Table,maps:remove(Ref, Queue)}
    end.

% Returns {NewTable,NewQueue,NextTimeout}
purge(Table, Queue) ->
    Now = calendar:datetime_to_gregorian_seconds(calendar:local_time()),
    try gb_trees:smallest(Table) of
        {Timeout, {Ref,Pid}} ->
            if Now >= Timeout -> 
                Pid ! {Ref,consistency_timeout},
                NewTable = gb_trees:delete(Timeout, Table),
                NewQueue = maps:remove(Ref, Queue),
                NextTimeout = try gb_trees:smallest(NewTable) of
                  {T,_} -> T
                catch
                  _:_ -> infinity
                end,
                {NewTable,NewQueue,NextTimeout};
            true -> {Table,Queue,Timeout} 
            end
    catch   % Table is empty
      _:_ -> {Table,Queue,infinity} 
    end.