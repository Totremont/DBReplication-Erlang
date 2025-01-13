-module(main).
-define(WAIT, 5000).
-export([init/0,start/2,stop/1]).


% Interfaces
init() -> 
    Pid = spawn(fun() -> loop(maps:new()) end), 
    try register(coordinator, Pid) of 
        _ -> ok
    catch
        _:_ -> exit(Pid,normal), already_running
    end.
        
start(Name,N) ->
    call({self(),start,{Name,N}}).

stop(Name) ->
    call({self(),stop,Name}).

call(Request) -> 
    case whereis(coordinator) of
        undefined -> exit({error,"No coordinator found. Call init() to get a new instance."});
        Pid -> 
            Pid ! Request,
            receive
                R -> R 
                after ?WAIT -> exit({error,"Coordinator did not return."})
            end
    end.


% Coordinador | State is a map | Responses are of type {response,method,args = {...}}
loop(State) -> 
    receive
        % Start 
        {Pid,start,{Name,N}} ->
            if (is_atom(Name) andalso N >= 0) ->
                case spawner:start(Name,N) of
                    {ok,Refs} -> Pid ! {ok,start,{Name,N}}, loop(maps:put(Name, Refs, State));
                    name_taken -> Pid ! {name_taken,start,{Name,N}};
                    time_out -> Pid ! {error,start,{Name,N}}
                end;
            true -> Pid ! {invalid_args,start,{Name,N}} 
            end;

        % Stop (all)
        {Pid,stop,Name} -> 
            if is_atom(Name) ->
                case maps:find(Name,State) of
                {ok,Refs} -> 
                    maps:foreach(fun(_,{Pid0,Ref}) -> exit(Pid0,normal),demonitor(Ref) end,Refs),
                    Pid ! {ok,stop,Name},
                    loop(maps:remove(Name, State));
                error -> 
                    Pid ! {notfound,stop,Name}
                end;
            true -> Pid ! {invalid_args,stop,Name} 
            end

    end,

    loop(State).
