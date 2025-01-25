-module(main).
-define(WAIT, 5000).
-export([init/0,start/2,startWithDummy/2,shutdown/0,stop/1,stop/0,familyInfo/1,put/3,get/3,delete/3,size/2,inspect/1]).
-include("types.hrl").


% Interfaces
init() -> 
    Pid = spawn(fun() -> build() end), 
    try register(coordinator, Pid) of 
        true -> ok
    catch
        _:_ -> exit(Pid,normal), already_running
    end.
        
start(Name,N) ->
    call({self(),start,{Name,N}}).

startWithDummy(Name,N) ->
    call({self(),start_dummy,{Name,N}}).

stop(Name) ->
    call({self(),stop,Name}).
stop() ->
    call({self(),stop}).

shutdown() -> 
    coordinator ! shutdown,
    ok.

familyInfo(Family) ->
    call({self(),info,Family}).


% {Pid, Operation, Params, Lider, Consistency,Ref}
put(Params = {_Key,_Value,_Timestamp},Lider,Consistency) ->
    Ref = make_ref(),
    call({self(),put,Params,Lider,Consistency,Ref}).

get(Key,Lider,Consistency) ->
    Ref = make_ref(),
    call({self(),get,Key,Lider,Consistency,Ref}).

delete(Params = {_Key,_Timestamp},Lider,Consistency) ->
    Ref = make_ref(),
    call({self(),delete,Params,Lider,Consistency,Ref}).

size(Lider,Consistency) ->
    Ref = make_ref(),
    call({self(),size,{},Lider,Consistency,Ref}).

inspect(Lider) ->
    Ref = make_ref(),
    call({self(),inspect,{},Lider,one,Ref}).
call(Request) -> 
    case whereis(coordinator) of
        undefined -> {error,"No coordinator found. Call init() to get a new instance."};
        Pid -> 
            Pid ! Request,
            receive
                R -> R 
                after ?WAIT -> {error,"Coordinator did not return."}
            end
    end.


% Coordinador | State is a map | Responses are of type {response,method,args = {...}} or {response,Ref}.

build() -> 
    process_flag(trap_exit, true),
    file:write_file(
        "logs.txt", 
        io_lib:fwrite("(~p) INFO: New coordinator running.\n", 
        [calendar:local_time()]),
        [append]
    ),
    loop(maps:new()).
loop(State) -> 
    receive
        % Start 
        {Pid,start,{Family,N}} ->
            if (is_atom(Family) andalso N >= 0) ->
                case spawner:start(Family,N) of
                    {ok,Refs,Group} -> 
                        NewState = saveState(State, Family, Refs),
                        Pid ! {ok,start,{Family,Group}},
                        loop(NewState);
                    {name_taken,_,_} -> Pid ! {name_taken,start,{Family,N}};
                    {timeout,_,_} -> Pid ! {error,start,{Family,N}}
                end;
            true -> Pid ! {invalid_args,start,{Family,N}}
            end;

        {Pid,start_dummy,{Family,N}} ->
            if (is_atom(Family) andalso N >= 0) ->
                case spawner:startWithDummy(Family,N) of
                    {ok,Refs,Group} -> 
                        NewState = saveState(State, Family, Refs),
                        Pid ! {ok,start_dummy,{Family,Group}},
                        loop(NewState);
                    {name_taken,_,_} -> Pid ! {name_taken,start_dummy,{Family,N}};
                    {timeout,_,_} -> Pid ! {error,start_dummy,{Family,N}}
                end;
            true -> Pid ! {invalid_args,start_dummy,{Family,N}}
            end;

        % Stop a family
        {Pid,stop,Family} -> 
            if is_atom(Family) ->
                case maps:find(Family,State) of
                {ok,Refs} -> 
                    Members = maps:size(Refs),
                    Pid ! {ok,stop,{Family,Members}},
                    loop(removeFamily(State, Family, Refs));
                error -> 
                    Pid ! {notfound,stop,Family}
                end;
            true -> Pid ! {invalid_args,stop,Family} 
            end;
        
        % Stop all
        {Pid,stop} ->
            case maps:size(State) of
                0 -> Pid ! {empty,stop};
                _ ->
                {Families,NewState} = removeAll(State),
                Pid ! {ok,stop,Families},
                loop(NewState)
            end;

        {Pid, Operation, Params, Lider, Consistency,Ref} ->
            Rpid = whereis(Lider),
            case maps:find(Rpid,State) of
                {ok,{_,_,_}} ->
                    % Pass client's Pid and Ref
                    Rpid ! {self(), {Pid,Ref}, {Operation, Params}, Consistency},
                    Pid ! {ok,Operation,Params,Consistency,Lider,Ref};
                _ -> Pid ! {lider_notfound,Operation,Lider}
            end;
        
        % Response from replica received
        #repl{ref = {Pid,Ref}, response = {Status,_,Result}, name = Name,timestamp = Timestamp} ->
            if is_integer(Result) -> Pid ! {{Status,Result,Name,Timestamp},Ref};
            Result =:= nil -> Pid ! {{Status,Name,Timestamp},Ref};
            % It's #data
            true -> Pid ! {{Status,Result,Name},Ref}
            end;
        % Timeout from replica
        {{Pid,Ref},timeout} ->
            Pid ! {timeout,Ref};
        
        % Someone died. Eliminate it from list.
        {'DOWN',_,process,Pid,_} -> 
            case maps:find(Pid,State) of
              {ok,{Family,Name,_}} -> 
                {ok,Group} = maps:find(Family, State),
                file:write_file(
                    "logs.txt", 
                    io_lib:fwrite("(~p) WARN: process [~p] from family [~p] died.\n", [calendar:local_time(),Name,Family]),
                    [append]
                ),
                NewGroup = maps:remove(Name, Group),
                loop(maps:remove(Pid, maps:put(Family, NewGroup, State)));
                error -> file:write_file(
                    "logs.txt", 
                    io_lib:fwrite("(~p) WARN: An orphan process died with Pid [~p].\n", 
                    [calendar:local_time(),Pid]),
                    [append]
                    )
            end;
        shutdown -> file:write_file(
            "logs.txt", 
            io_lib:fwrite("(~p) INFO: Coordinator was shut down.\n", 
            [calendar:local_time()]),
            [append]
            ),
            exit(normal);

        {Pid,info,Family} ->
            case maps:find(Family,State) of
            {ok,Val} -> 
                Members = maps:size(Val),
                Names = maps:keys(Val),
                Pid ! {ok,Family,{Members,Names}};
            error -> Pid ! {notfound,Family}
            end;
        % Flush anything else
        _ -> loop(State)
        end,

    loop(State).

% Adds to state ({K,V}) -> {Pid,{Family,Name,Ref}}
saveState(State, Family, Refs) ->
    maps:fold
    (
        fun(Pname,{Pid,Ref},Acc) -> maps:put(Pid,{Family,Pname,Ref},Acc) end, 
        maps:put(Family,Refs,State),
        Refs
    ).

removeFamily(State,Family,Refs) ->
    maps:fold
    (
        fun(Pname,{Pid,Ref},Acc) -> 
            demonitor(Ref,[flush]), exit(whereis(Pname),kill), maps:remove(Pid, Acc) end, 
        maps:remove(Family,State),
        Refs
    ).

removeAll(State) ->
    maps:fold(
        fun(K,V,Acc) -> 
            {Families,Curr} = Acc,
            if is_atom(K) ->
                Members = maps:size(V),
                {[{K,Members} | Families], maps:remove(K, Curr)};
            true -> % K is pid
                {_,_,Ref} = V,
                demonitor(Ref,[flush]), exit(K,kill), {Families,maps:remove(K, Curr)}
            end
        end,
        {[],State},
        State
    ).