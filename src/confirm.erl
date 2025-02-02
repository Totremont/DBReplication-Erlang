-module(confirm).
-include("types.hrl").
-define(TIMEOFFSET, 5). % 5 seconds
-export([init/0,onConfirm/2,saveRequest/5,purge/2]).

% Returns new {Table,Queue,Timeout}
init() -> 
    Self = self(),
    spawn_link(fun() -> server(Self,gb_trees:empty(),infinity) end).

% Save requests that have pending confirmation messages.    
saveRequest(ClientPid,MyResponse = #repl{ref = Ref},Group,Queue,ConfirmServer) ->
    if Group =:= [] ->
        ClientPid ! MyResponse,
        Queue;
    true ->
        {_,Now,_} = erlang:timestamp(),  % 5 seconds after
        Timeout = Now + ?TIMEOFFSET,
        ConfirmServer ! {Ref,Timeout},
        maps:put(Ref, {ClientPid,Group,MyResponse}, Queue)
    end.

% A confirmation message arrived
% Returns Queue
onConfirm(NewResponse = #repl{ref = Ref, name = Identity, timestamp = NewTime}, Queue) ->

    case maps:find(Ref, Queue) of
        {ok,{Pid,Pending,CurrResponse = #repl{timestamp = CurrTime}}} ->
            UpdatedPending = lists:delete(Identity, Pending),
            UpdatedResponse = 
            case operators:getNewerDate(CurrTime,NewTime) of
                CurrTime -> CurrResponse;
                NewTime -> NewResponse
            end,
            if UpdatedPending =:= [] ->
                Pid ! UpdatedResponse,
                maps:remove(Ref, Queue);
            true ->
                maps:put(Ref,{Pid,UpdatedPending,UpdatedResponse},Queue)
            end;
        % Deleted by a purge. Drop message
        error -> Queue
    end.

% Removes expired request. 
% Returns Queue
purge(Queue,#exp{ref = Ref}) ->
    case maps:find(Ref,Queue) of
        {ok,{Pid,_,_}} -> 
            Pid ! {Ref,timeout},
            maps:remove(Ref,Queue);
        error -> Queue
    end.

% A synchonize() method may be required if it's suspected that 
% the replica and the timeout process have out-of-sync clocks.
server(Pid,Queue,NextTimeout) ->

    receive
    {NewRef,NewTimeout} ->              
        {NewQueue, Timeout} = refreshQueue(Pid, Queue), % First check if any request expired
        if Timeout =:= infinity ->
            {_,Now,_} = erlang:timestamp(),
            Diff = NewTimeout - Now,
            server(Pid,gb_trees:enter(NewTimeout, NewRef, NewQueue),if Diff > 0 -> Diff*1000; true -> 0 end);
        true -> server(Pid,gb_trees:enter(NewTimeout, NewRef, NewQueue),Timeout)
        end

    after NextTimeout ->
        {NewQueue, Timeout} = refreshQueue(Pid, Queue),
        server(Pid,NewQueue,Timeout)
    end.

% Returns {Queue,NextTimeout}
refreshQueue(Pid,Queue) ->
    try gb_trees:smallest(Queue) of
        {0,nil} -> {Queue,infinity};    % Is empty
        {Timeout,Ref} ->
            {_,Now,_} = erlang:timestamp(),
            if Now >= Timeout ->
                Pid ! #exp{ref = Ref},
                refreshQueue(Pid,gb_trees:delete(Timeout, Queue));
            true -> 
                Diff = Timeout - Now,
                {Queue,if Diff > 0 -> Diff*1000; true -> 0 end}
            end
    catch
      _:_ -> {Queue,infinity}    % Is empty
    end.