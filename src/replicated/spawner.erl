-module(spawner).
-define(DASH, 45).      % "-" symbol
-define(WAIT,5000).
-define(OFFSET,48).     % Offset to get character encode.
-export([start/2]).

%% Args: FamilyName, N
%% Returns: {ok, Refs} Refs :: map(name,{Pid,Ref}) | name_taken | time_out

% Interfaces
start(Family,N) -> 
    {ok,Acc} = setNames(Family,N,[],1),                     % Set proccesses' names
    {ok,Refs} = startProcesses(Acc,Acc,maps:new()),         % Spawn processes
    listen(Refs,Refs).                                           % Wait for confirmation


%% Implementation
listen(Refs,G) -> 
    case maps:size(Refs) of
        0 -> {ok,G};
        _ ->
            receive 
                {ok,Name} -> listen(maps:remove(Name, Refs),G);
                name_taken -> abort(Refs), name_taken;
                _ -> listen(Refs,G)
            after ?WAIT -> 
                abort(Refs),
                time_out
            end
    end.

%% Implementations
setNames(Family,N,Acc,Index) when Index =< N -> 
    Aux = list_to_atom(atom_to_list(Family) ++ [?DASH, (Index + ?OFFSET)]),
    setNames(Family,N,[Aux | Acc],(Index + 1));

setNames(_,_,Acc,_) -> {ok,Acc}.

startProcesses(_,[],Refs) -> {ok,Refs};

startProcesses(Acc,[H | T],Refs) ->
    Ref = spawn_monitor(replica,start,[self(),H,Acc]),
    startProcesses(Acc,T,maps:put(H, Ref, Refs)).

% Kill whatever is alive
abort(Refs) -> maps:map(fun(_,{Pid,Ref}) -> exit(Pid,normal), demonitor(Ref) end, Refs).